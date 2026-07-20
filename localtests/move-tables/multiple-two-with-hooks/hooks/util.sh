#/bin/bash

assert_env_equal() {
    env_name=$1
    expected=$2

    if [ "${!env_name}" != "${expected}" ]; then
        echo "ERROR: Expected '${expected}' for ${env_name}, but got '${!env_name}'"
        exit 1
    fi
}

assert_env_present() {
    env_name=$1

    echo "checking '${env_name}=${!env_name}'"
    if [[ -z "${!env_name}" ]]; then
        echo "ERROR: Expected '${env_name}' to be set but not present"
        exit 1
    fi
}

# Assert a table-list env var holds ONLY comma-separated table names, with no
# schema/database prefix. gh-ost must expose bare table names (e.g.
# "gh_ost_test,gh_ost_test_other"), never schema-qualified ones (e.g.
# "test.gh_ost_test,test.gh_ost_test_other"). A '.' in any element signals a
# leaked schema prefix — the db-infra-scripts hooks build `db`.`table` grants
# themselves and would double-qualify if gh-ost passed a prefix.
assert_no_schema_prefix() {
    env_name=$1
    value="${!env_name}"

    echo "checking '${env_name}' has no schema prefix: '${value}'"

    local IFS=','
    for element in ${value}; do
        if [[ "${element}" == *.* ]]; then
            echo "ERROR: ${env_name} element '${element}' is schema-qualified; expected a bare table name"
            exit 1
        fi
    done
}

# Assert the environment contract gh-ost exposes to hooks for a multi-table move.
# The table-name variables are comma-joined lists (in tables.txt order), with the
# rollback handles being the per-table `_<table>_del` names produced by the atomic
# cutover RENAME. This mirrors go/logic/hooks.go:applyEnvironmentVariables and is
# the exact contract the db-infra-scripts move-tables hooks rely on (a regression
# here is what caused the per-table GRANT bug).
assert_common_envs() {
    assert_env_present "GH_OST_TARGET_HOST"

    assert_env_equal "GH_OST_TARGET_DATABASE_NAME" "test"
    assert_env_equal "GH_OST_TABLE_NAME" "gh_ost_test,gh_ost_test_other"
    assert_env_equal "GH_OST_GHOST_TABLE_NAME" "gh_ost_test,gh_ost_test_other"
    assert_env_equal "GH_OST_TARGET_TABLE_NAME" "gh_ost_test,gh_ost_test_other"
    assert_env_equal "GH_OST_OLD_TABLE_NAME" "_gh_ost_test_del,_gh_ost_test_other_del"
    assert_env_equal "GH_OST_TABLES" "gh_ost_test,gh_ost_test_other"
    assert_env_equal "GH_OST_MOVE_TABLES" "true"
    assert_env_equal "GH_OST_REVERT" "false"

    # The table lists must be bare, comma-separated table names — never
    # schema-qualified (the database is carried separately in
    # GH_OST_TARGET_DATABASE_NAME).
    assert_no_schema_prefix "GH_OST_TABLE_NAME"
    assert_no_schema_prefix "GH_OST_GHOST_TABLE_NAME"
    assert_no_schema_prefix "GH_OST_TARGET_TABLE_NAME"
    assert_no_schema_prefix "GH_OST_OLD_TABLE_NAME"
    assert_no_schema_prefix "GH_OST_TABLES"
}

dump_env() {
    echo "-----------------------------------------------------"
    echo "----------------- ENVIRONS --------------------------"
    echo "-----------------------------------------------------"
    env | grep "GH_OST_"
    echo "-----------------------------------------------------"
    echo "-----------------------------------------------------"
    echo "-----------------------------------------------------"
}
