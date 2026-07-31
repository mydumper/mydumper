/*
 * test_string_pk_planner_utils.c
 *
 * Fast unit test for the pure string-PK planner helpers.
 * This does not require a live MySQL server.
 */

#include <glib.h>
#include <stdio.h>

#include "../../src/mydumper/mydumper_string_planner_utils.h"

static int total_failed = 0;

#define ASSERT_TRUE(label, expr)                                              \
  do {                                                                        \
    if (!(expr)) {                                                            \
      fprintf(stderr, "  FAIL [%s]\n", (label));                              \
      total_failed++;                                                         \
    } else {                                                                  \
      fprintf(stdout, "  pass [%s]\n", (label));                              \
    }                                                                         \
  } while (0)

#define ASSERT_EQUAL(label, actual, expected)                                 \
  do {                                                                        \
    if ((actual) != (expected)) {                                             \
      fprintf(stderr, "  FAIL [%s]: expected %llu, got %llu\n",              \
              (label),                                                        \
              (unsigned long long)(expected),                                 \
              (unsigned long long)(actual));                                  \
      total_failed++;                                                         \
    } else {                                                                  \
      fprintf(stdout, "  pass [%s]: %llu == %llu\n",                         \
              (label),                                                        \
              (unsigned long long)(actual),                                   \
              (unsigned long long)(expected));                                \
    }                                                                         \
  } while (0)

static void test_strategy_parser(void){
  enum string_pk_planner_strategy strategy = STRING_PK_PLANNER_AUTO;

  ASSERT_TRUE("parse auto", string_pk_planner_strategy_from_string("auto", &strategy));
  ASSERT_EQUAL("auto enum", strategy, STRING_PK_PLANNER_AUTO);
  ASSERT_TRUE("parse metadata", string_pk_planner_strategy_from_string("metadata", &strategy));
  ASSERT_EQUAL("metadata enum", strategy, STRING_PK_PLANNER_METADATA);
  ASSERT_TRUE("parse recursive", string_pk_planner_strategy_from_string("recursive", &strategy));
  ASSERT_EQUAL("recursive enum", strategy, STRING_PK_PLANNER_RECURSIVE);
  ASSERT_TRUE("reject invalid", !string_pk_planner_strategy_from_string("nope", &strategy));
}

static void test_strategy_decision(void){
  ASSERT_TRUE("auto enabled on large tables",
              string_pk_planner_should_use_metadata_mode(STRING_PK_PLANNER_AUTO, TRUE, TRUE, 2000000, 1000000));
  ASSERT_TRUE("auto disabled on small tables",
              !string_pk_planner_should_use_metadata_mode(STRING_PK_PLANNER_AUTO, TRUE, TRUE, 100, 1000000));
  ASSERT_TRUE("recursive disables metadata",
              !string_pk_planner_should_use_metadata_mode(STRING_PK_PLANNER_RECURSIVE, TRUE, TRUE, 2000000, 1000000));
  ASSERT_TRUE("metadata forces planner",
              string_pk_planner_should_use_metadata_mode(STRING_PK_PLANNER_METADATA, TRUE, TRUE, 100, 1000000));
  ASSERT_TRUE("disabled string PK planner",
              !string_pk_planner_should_use_metadata_mode(STRING_PK_PLANNER_METADATA, TRUE, FALSE, 2000000, 1000000));
}

static void test_root_step(void){
  ASSERT_EQUAL("rows evenly divided", string_pk_planner_compute_root_step(1000, 10, 100), 100);
  ASSERT_EQUAL("minimum step enforced", string_pk_planner_compute_root_step(10, 100, 50), 50);
  ASSERT_EQUAL("fallback to one", string_pk_planner_compute_root_step(0, 0, 0), 1);
}

static void test_compute_target(void){
  /* target=0 -> derive rows/max_prefixes */
  ASSERT_EQUAL("derived from max-prefixes",
               string_pk_planner_compute_target(1000000, 0, 1000, 0), 1000);
  /* explicit target > 0 is used directly */
  ASSERT_EQUAL("explicit target used",
               string_pk_planner_compute_target(1000000, 5000, 1000, 0), 5000);
  /* explicit target overrides the derived value */
  ASSERT_EQUAL("explicit target overrides derived",
               string_pk_planner_compute_target(1000000, 250, 1000, 0), 250);
  /* min_chunk_step_size acts as a floor */
  ASSERT_EQUAL("min chunk step floor",
               string_pk_planner_compute_target(1000000, 10, 1000, 100), 100);
  /* never returns zero even when inputs would divide to zero */
  ASSERT_EQUAL("never zero",
               string_pk_planner_compute_target(10, 0, 1000, 0), 1);
  /* max_prefixes=0 with no explicit target -> whole table is the target */
  ASSERT_EQUAL("no prefixes cap falls back to rows",
               string_pk_planner_compute_target(777, 0, 0, 0), 777);
}

static void test_level_fits_budget(void){
  ASSERT_TRUE("within budget", string_pk_planner_level_fits_budget(100, 256));
  ASSERT_TRUE("equal to budget fits", string_pk_planner_level_fits_budget(256, 256));
  ASSERT_TRUE("over budget rejected", !string_pk_planner_level_fits_budget(257, 256));
  ASSERT_TRUE("zero budget is unbounded", string_pk_planner_level_fits_budget(1000000, 0));
}

int main(void){
  test_strategy_parser();
  test_strategy_decision();
  test_root_step();
  test_compute_target();
  test_level_fits_budget();

  if (total_failed != 0) {
    fprintf(stderr, "\nFAILED: %d assertion(s)\n", total_failed);
    return 1;
  }

  fprintf(stdout, "\nPASS\n");
  return 0;
}
