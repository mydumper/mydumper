// -*- mode: C; c-basic-offset: 8; tab-width: 8; indent-tabs-mode: t; -*-
/*
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

	Authors: 	Taneli Leppä <taneli.leppae@rocket-internet.de>

*/

#include <glib.h>
#include <glib/gstdio.h>
#include <errno.h>
#include <yaml.h>
#include <mysql.h>
#include <pcre.h>
#include "mydumper.h"

GNode *anonymizer_config = NULL;
gboolean anonymize = FALSE;
GHashTable *randomizer_lists = NULL;
GRand *prng = NULL;

gboolean read_anonymizer_config(char *config_file);
gboolean dump(GNode *node, gpointer data);
void process_yaml(yaml_parser_t *parser, GNode *data);
GNode *get_table_anonymization(char *database, char *table);
gboolean find_gnode_by_string(GNode *node, gpointer data);
gboolean read_randomizer_lists();
void replace_column_contents(const char *value, MYSQL_FIELD *fields, int num_fields, MYSQL_ROW row, gulong *lengths, gboolean *changed, gint column_index);

enum storage_flags { VAR, VAL, SEQ }; // "Store as" switch

typedef struct _gnode_search {
	char *needle;
	GNode *found_node;
} gnode_search;

void free_randomizer_list(gpointer key, gpointer value, gpointer user_data)
{
	GArray *wordlist = (GArray *)value;
	guint i;
	char *s;

	(void)key; (void)user_data;
	for (i = 0; i < wordlist->len; i++) {
		s = &g_array_index(wordlist, char, i);
		free(s);
	}
	g_array_free(wordlist, TRUE);
}

void free_randomizer_lists()
{
	if (randomizer_lists != NULL) {
		g_hash_table_foreach(randomizer_lists, free_randomizer_list, NULL);
		g_hash_table_destroy(randomizer_lists);
		randomizer_lists = NULL;
	}
}

gboolean free_anonymizer_config_node(GNode *node, gpointer data)
{
	(void)data;
	if (node->data != NULL) {
		free(node->data);
		node->data = NULL;
	}
	return FALSE;
}

void free_anonymizer_config()
{
	if (anonymizer_config != NULL) {
		g_node_traverse(anonymizer_config, G_IN_ORDER, G_TRAVERSE_ALL, -1, free_anonymizer_config_node, NULL);
		g_node_destroy(anonymizer_config);
		anonymizer_config = NULL;
	}
}

#define MAX_WORD_LENGTH 1024
gboolean read_randomizer_list(const char *filename, const char *listname)
{
	FILE *file = NULL;
	gpointer list = NULL;
	GArray *wordlist = NULL;
	char s[MAX_WORD_LENGTH] = { '\0' };
	char *scopy = NULL;
	unsigned long i = 0;

	file = fopen(filename, "rt");
	if (file == NULL) {
		g_critical("Error: Could not read anonymization randomizer list from: %s: %s (%s)", listname, filename, strerror(errno));
		return FALSE;
	}

	if (randomizer_lists == NULL) {
		randomizer_lists = g_hash_table_new(g_str_hash, g_str_equal);
	}

	list = g_hash_table_lookup(randomizer_lists, (gpointer)listname);
	if (list != NULL) {
		g_critical("Error: Anonymization randomizer list already loaded: %s: %s", listname, filename);
		return FALSE;
	}

	wordlist = g_array_new(FALSE, FALSE, sizeof(char *));
	while (fgets(s, MAX_WORD_LENGTH, file) != NULL) {
		if (strlen(s) > 1) {
			s[strlen(s) - 1] = '\0'; // remove end of line
			for (i = 0; i < strlen(s); i++)
			{
				if (s[i] != ' ') {
					scopy = strdup((const char *)s);
					wordlist = g_array_append_val(wordlist, scopy);
					break;
				}
			}
		}

	}
	g_hash_table_replace(randomizer_lists, (gpointer)listname, (gpointer)wordlist);
	fclose(file);
	return TRUE;
}

gboolean find_gnode_by_string(GNode *node, gpointer data)
{
	gnode_search *search = (gnode_search *)data;
	if (strcmp(search->needle, (char *)node->data) == 0) {
		search->found_node = node;
		return TRUE;
	}
	return FALSE;
}

gboolean get_yaml_boolean(char *value)
{
	if (g_ascii_strcasecmp((const gchar *)value, "yes") == 0 ||
	    g_ascii_strcasecmp((const gchar *)value, "y") == 0 ||
	    g_ascii_strcasecmp((const gchar *)value, "true") == 0 ||
	    g_ascii_strcasecmp((const gchar *)value, "on") == 0)
		return TRUE;
	return FALSE;
}

gboolean should_truncate_table(GNode *table)
{
	gnode_search truncate_node = { 0, 0 };
	GNode *value = NULL;

	truncate_node.needle = (char *)"truncate";
	g_node_traverse(table, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&truncate_node);
	if (truncate_node.found_node != NULL) {
		value = g_node_first_child(truncate_node.found_node);
		if (value != NULL && get_yaml_boolean(value->data)) {
			return TRUE;
		}
	}
	return FALSE;
}

gboolean has_columns_to_anonymize(GNode *table)
{
	gnode_search edit_node = { 0, 0 };

	edit_node.needle = (char *)"edit";
	g_node_traverse(table, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&edit_node);
	if (edit_node.found_node != NULL) {
		return TRUE;
	}

	edit_node.needle = (char *)"randomize";
	g_node_traverse(table, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&edit_node);
	if (edit_node.found_node != NULL) {
		return TRUE;
	}

	return FALSE;
}

void replace_column_contents(const char *value, MYSQL_FIELD *fields, int num_fields, MYSQL_ROW row, gulong *lengths, gboolean *changed, gint column_index)
{
	gchar *result = NULL, *lastresult = NULL, *value_copy = NULL, *modifier = NULL, *final_value = NULL;
	unsigned long i = 0, last = 0, o = 0, start = 0, end = 0;
	int found_index = -1;
	char *sub_str = NULL;
	int sub_pos = 0;
	GChecksum *cksum = NULL;

	// shortest that needs replacing is "{{a}}"
	if (strlen(value) < 5) {
		row[column_index] = strdup(value);
		lengths[column_index] = strlen(value);
		changed[column_index] = TRUE;
		return;
	}

	value_copy = g_strdup((const gchar *)value);
	last = 0;
	for (i = 1; i < strlen(value); i++) {
		if ((value[i-1] == '{' && value[i] == '{') && (i+2) < strlen(value)) {
			start = i + 1;
			end = 0;
			for (o = i + 2; o < strlen(value); o++) {
				if (value[o] == '}' && value[o+1] == '}') {
					end = o - 1;
					i = o + 1;
					break;
				}
			}
			if (end == 0) {
				// Stray {{
				break;
			}
			lastresult = result;

			modifier = NULL;
			value_copy[end + 1] = '\0';
			for (o = start; o < strlen(value_copy); o++) {
				if (value_copy[o] == '|') {
					modifier = (value_copy + o + 1);
					value_copy[o] = '\0';
					break;
				}
			}

			found_index = -1;
			for (o = 0; o < (unsigned long)num_fields; o++) {
				if (strcmp(fields[o].name, (value_copy + start)) == 0) {
					found_index = o;
					break;
				}
			}
			if (found_index != -1) {
				if (row[found_index] == NULL) {
					final_value = (gchar *)g_strdup("");
				} else {
					final_value = (gchar *)g_strdup(row[found_index]);
				}

				if (modifier != NULL) {
					if (strncmp(modifier, "md5", 3) == 0) {

						cksum = g_checksum_new(G_CHECKSUM_MD5);
						g_checksum_update(cksum, (const guchar *)final_value, strlen(final_value));

						g_free(final_value);
						final_value = g_strdup(g_checksum_get_string(cksum));
						g_checksum_free(cksum);
					}
					sub_str = strstr(modifier, ":");
					if (sub_str != NULL) {
						sub_str++;
						sub_pos = atoi(sub_str);
						if (sub_pos > 0 && sub_pos < (int)strlen(final_value)) {
							*(final_value + sub_pos) = '\0';
						}
					}
				}
			} else {
				final_value = NULL;
			}


			value_copy[start - 2] = '\0';
			if (result == NULL) {
				result = g_strconcat((gchar *)(value_copy + last),
						     final_value,
						     NULL);
			} else {
				result = g_strconcat(result,
						     (gchar *)(value_copy + last),
						     final_value,
						     NULL);
			}
			if (final_value != NULL) {
				g_free(final_value);
				final_value = NULL;
			}
			if (lastresult != NULL) {
				g_free(lastresult);
				lastresult = NULL;
			}
			last = end + 3;
		}
	}

	// Append whatever was left
	value_copy[start - 2] = '\0';
	if (result == NULL) {
		result = g_strconcat((gchar *)(value_copy + last),
				     NULL);
	} else {
		result = g_strconcat(result,
				     (gchar *)(value_copy + last),
				     NULL);
	}

	if (value_copy != NULL) {
		free(value_copy);
	}

	if (lastresult != NULL) {
		g_free(lastresult);
		lastresult = NULL;
	}
	last = end + 1;

	if (changed[column_index]) {
		g_free(row[column_index]);
	}
	row[column_index] = result;
	lengths[column_index] = strlen(result);
	changed[column_index] = TRUE;
}

void edit_table_columns(GNode *edit_cfg, MYSQL_FIELD *fields, int num_fields, MYSQL_ROW row, gulong *lengths, gboolean *changed)
{
	gnode_search when_node = { 0, 0 };
	gnode_search set_node = { 0, 0 };
	guint i = 0, o = 0, u = 0;
	gint column_index = -1;
	gboolean all_match = FALSE;
	GNode *field_node = NULL;
	GNode *value_node = NULL;
	char *field_name = NULL;
	pcre *re = NULL;
	const char *error;
	int erroroffset;
	int ovector[9] = {0};
	int rc;


	for (i = 0; i < g_node_n_children(edit_cfg); i++) {
		when_node.needle = (char *)"when";
		g_node_traverse(g_node_nth_child(edit_cfg, i), G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&when_node);
		if (when_node.found_node != NULL) {
			all_match = TRUE;

			// loop through fields to see if they all match
			for (o = 0; o < g_node_n_children(when_node.found_node); o++) {
				field_node = g_node_nth_child(when_node.found_node, o);
				field_name = (char *)field_node->data;

				column_index = -1;
				for (u = 0; u < (guint)num_fields; u++) {
					if (strcmp((const char *)fields[u].name, field_name) == 0) {
						column_index = u;
						break;
					}
				}
				if (column_index == -1) {
					g_critical("Anonymizer: missing column for edit: %s", field_name);
					exit(EXIT_FAILURE);
				}

				value_node = g_node_first_child(field_node);
				if (value_node != NULL && row[column_index] != NULL &&
				    !(fields[column_index].flags & NUM_FLAG)) {
					re = pcre_compile((char *)value_node->data, 0, &error, &erroroffset, NULL);
					if (!re) {
						g_critical("Anonymizer: Regular expression %s fail: %s", (char *)value_node->data, error);
						exit(EXIT_FAILURE);
					}

					rc = pcre_exec(re, NULL, row[column_index], lengths[column_index], 0, 0, ovector, 9);
					pcre_free(re);

					if (rc == -1) {
						all_match = FALSE;
					}
				} else {
					all_match = FALSE;
				}

			}
			// All regexpes matched, edit the row
			if (all_match) {
				set_node.needle = (char *)"set";
				g_node_traverse(g_node_nth_child(edit_cfg, i), G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&set_node);
				if (set_node.found_node != NULL) {
					// loop through fields to see if they all match
					for (o = 0; o < g_node_n_children(set_node.found_node); o++) {
						field_node = g_node_nth_child(set_node.found_node, o);
						field_name = (char *)field_node->data;
						value_node = g_node_first_child(field_node);

						column_index = -1;
						for (u = 0; u < (guint)num_fields; u++) {
							if (strcmp((const char *)fields[u].name, field_name) == 0) {
								column_index = u;
								break;
							}
						}
						if (column_index == -1) {
							g_critical("Anonymizer: missing set column for edit: %s", field_name);
							exit(EXIT_FAILURE);
						}

						replace_column_contents((gchar *)value_node->data, fields, num_fields, row, lengths, changed, column_index);
					}
				}
			}
		}
	}

}

#define MAX_TIMESTAMP_LENGTH 20
void randomize_table_columns(GNode *randomize, MYSQL_FIELD *fields, int num_fields, MYSQL_ROW row, gulong *lengths, gboolean *changed)
{
	guint i = 0, u = 0;
	gint column_index = -1;
	GNode *field_node = NULL;
	char *field_name = NULL;
	GNode *random_type = NULL;
	gnode_search type_node = { 0, 0 };
	GDate *random_day = NULL, *today = NULL;
	gint32 rnd = 0, hour = 0, min = 0, sec = 0;
	char tsstr[MAX_TIMESTAMP_LENGTH] = { '\0' };
	GArray *wordlist = NULL;
	char *word = NULL;

	if (prng == NULL) {
		prng = g_rand_new();
	}

	today = g_date_new();
	g_date_set_time_t(today, time(NULL));

	for (i = 0; i < g_node_n_children(randomize); i++) {
		field_node = g_node_nth_child(randomize, i);
		field_name = (char *)field_node->data;

		column_index = -1;
		for (u = 0; u < (guint)num_fields; u++) {
			if (strcmp((const char *)fields[u].name, field_name) == 0) {
				column_index = (gint)u;
				break;
			}
		}

		if (column_index != -1) {
			type_node.needle = (char *)"type";
			g_node_traverse(field_node, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&type_node);
			if (type_node.found_node != NULL) {
				random_type = g_node_first_child(type_node.found_node);
				if (random_type->data != NULL) {
					if (strcmp((const char *)random_type->data, "date") == 0 ||
					    strcmp((const char *)random_type->data, "datetime") == 0 ||
					    strcmp((const char *)random_type->data, "time") == 0) {
						random_day = g_date_new();
						g_date_set_dmy(random_day, 1, 1, 1970);

						// Add years
						rnd = g_rand_int_range(prng, 0, (g_date_get_year(today) - 1970));
						g_date_add_years(random_day, (guint)rnd);

						// Add months
						rnd = g_rand_int_range(prng, 0, 11);
						g_date_add_months(random_day, (guint)rnd);

						// Add days
						rnd = g_rand_int_range(prng, 0, 30);
						g_date_add_days(random_day, (guint)rnd);

						// Randomize time
						hour = g_rand_int_range(prng, 0, 23);
						min = g_rand_int_range(prng, 0, 59);
						sec = g_rand_int_range(prng, 0, 59);


						if (strcmp((const char *)random_type->data, "date") == 0) {
							g_date_strftime(tsstr, MAX_TIMESTAMP_LENGTH - 1, "%Y-%m-%d", random_day);
						} else if (strcmp((const char *)random_type->data, "time") == 0) {
							g_snprintf(tsstr, MAX_TIMESTAMP_LENGTH - 1, "%02d:%02d:%02d", hour, min, sec);
						} else {
							g_date_strftime(tsstr, MAX_TIMESTAMP_LENGTH - 1, "%Y-%m-%d", random_day);
							g_snprintf((tsstr + 10), MAX_TIMESTAMP_LENGTH - 10, " %02d:%02d:%02d", hour, min, sec);
						}

						if (changed[column_index]) {
							g_free(row[column_index]);
						}
						row[column_index] = g_strdup(tsstr);
						lengths[column_index] = strlen(tsstr);
						changed[column_index] = TRUE;
					} else {
						wordlist = (GArray *)g_hash_table_lookup(randomizer_lists, (gpointer)random_type->data);
						if (wordlist == NULL) {
							g_critical("Error: Anonymization randomizer list not found: %s", (char *)random_type->data);
							return;
						}

						rnd = g_rand_int_range(prng, 0, wordlist->len - 1);
						word = g_array_index(wordlist, char *, rnd);
						if (changed[column_index]) {
							g_free(row[column_index]);
						}
						row[column_index] = g_strdup(word);
						lengths[column_index] = strlen(word);
						changed[column_index] = TRUE;

					}
				}

			}

		}
	}

}

void anonymize_table_columns(GNode *table, MYSQL_FIELD *fields, int num_fields, MYSQL_ROW row, gulong *lengths)
{
	gboolean *changed = NULL;
	gnode_search edit_node = { 0, 0 };
	gnode_search randomize_node = { 0, 0 };

	changed = malloc(sizeof(gboolean) * num_fields);
	memset(changed, '\0', sizeof(gboolean) * num_fields);

	edit_node.needle = (char *)"edit";
	g_node_traverse(table, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&edit_node);
	if (edit_node.found_node != NULL) {
		edit_table_columns(edit_node.found_node, fields, num_fields, row, lengths, changed);
	}

	randomize_node.needle = (char *)"randomize";
	g_node_traverse(table, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&randomize_node);
	if (randomize_node.found_node != NULL) {
		randomize_table_columns(randomize_node.found_node, fields, num_fields, row, lengths, changed);
	}

	free(changed);
}

GNode *get_table_anonymization(char *database, char *table)
{
	gnode_search db_node = { 0, 0 };
	gnode_search table_node = { 0, 0 };

	db_node.needle = database;
	g_node_traverse(anonymizer_config, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&db_node);

	if (db_node.found_node != NULL) {
		table_node.needle = table;
		g_node_traverse(db_node.found_node, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&table_node);

		if (table_node.found_node != NULL) {
			return table_node.found_node;
		}
	}

	db_node.needle = (char *)"_all";
	g_node_traverse(anonymizer_config, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&db_node);

	if (db_node.found_node != NULL) {
		table_node.needle = table;
		g_node_traverse(db_node.found_node, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&table_node);

		if (table_node.found_node != NULL) {
			return table_node.found_node;
		}
	}

	return NULL;
}

gboolean read_randomizer_lists()
{
	gnode_search settings_node = { 0, 0 };
	gnode_search randomizer_node = { 0, 0 };
	GNode *wordlist_node = NULL, *filename_node = NULL;
	guint i = 0;
	char *listname = NULL;
	char *filename = NULL;

	settings_node.needle = (char *)"anonymizer_settings";
	g_node_traverse(anonymizer_config, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&settings_node);

	if (settings_node.found_node != NULL) {
		randomizer_node.needle = (char *)"randomize";
		g_node_traverse(settings_node.found_node, G_IN_ORDER, G_TRAVERSE_ALL, 2, find_gnode_by_string, (gpointer)&randomizer_node);

		if (randomizer_node.found_node != NULL) {
			for (i = 0; i < g_node_n_children(randomizer_node.found_node); i++) {
				wordlist_node = g_node_nth_child(randomizer_node.found_node, i);
				listname = (char *)wordlist_node->data;
				filename_node = g_node_first_child(wordlist_node);
				filename = (char *)filename_node->data;

				if (listname != NULL && filename != NULL) {
					if (!read_randomizer_list(filename, listname)) {
						return FALSE;
					}
				}
			}
		}
	}
	return TRUE;

}

gboolean read_anonymizer_config(char *config_file)
{
	yaml_parser_t parser;

	FILE *file;

	file = fopen(config_file, "rb");
	if (file == NULL) {
		g_critical("Error: Could not read anonymization configuration from: %s: %s", config_file, strerror(errno));
		return FALSE;
	}

	anonymizer_config = g_node_new(config_file);

	yaml_parser_initialize(&parser);
	yaml_parser_set_input_file(&parser, file);

	process_yaml(&parser, anonymizer_config);

	yaml_parser_delete(&parser);

	anonymize = TRUE;

	if (!read_randomizer_lists()) {
		return FALSE;
	}

	//	g_node_traverse(anonymizer_config, G_PRE_ORDER, G_TRAVERSE_ALL, -1, dump, NULL);

	return TRUE;
}

// Based on example from mk-fg at Stackoverflow
void process_yaml(yaml_parser_t *parser, GNode *data) {
	GNode *last_leaf = data;
	yaml_event_t event;
	int storage = VAR;

	while (TRUE) {
		if (!yaml_parser_parse(parser, &event))
			break;

		if (event.type == YAML_SCALAR_EVENT) {
			if (storage) g_node_append_data(last_leaf, g_strdup((gchar*) event.data.scalar.value));
			else last_leaf = g_node_append(data, g_node_new(g_strdup((gchar*) event.data.scalar.value)));
			storage ^= VAL;
		}
		else if (event.type == YAML_SEQUENCE_START_EVENT) storage = SEQ;
		else if (event.type == YAML_SEQUENCE_END_EVENT) storage = VAR;
		else if (event.type == YAML_MAPPING_START_EVENT) {
			process_yaml(parser, last_leaf);
			storage ^= VAL;
		}
		else if (
			 event.type == YAML_MAPPING_END_EVENT
			 || event.type == YAML_STREAM_END_EVENT
			 || event.type == YAML_NO_EVENT
			 ) break;

		yaml_event_delete(&event);
	}
}


gboolean dump(GNode *node, gpointer data) {
	(void)data;

	int i = g_node_depth(node);
	while (--i) printf(" ");
	printf("%s\n", (char*) node->data);
	return(FALSE);
}
