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

int main(void){
  test_strategy_parser();
  test_strategy_decision();
  test_root_step();

  if (total_failed != 0) {
    fprintf(stderr, "\nFAILED: %d assertion(s)\n", total_failed);
    return 1;
  }

  fprintf(stdout, "\nPASS\n");
  return 0;
}
