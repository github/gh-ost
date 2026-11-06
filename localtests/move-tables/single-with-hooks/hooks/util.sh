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

assert_common_envs() {
    assert_env_present "GH_OST_TARGET_HOST"

    assert_env_equal "GH_OST_TARGET_DATABASE_NAME" "test"
    assert_env_equal "GH_OST_TABLE_NAME" "gh_ost_test"
    assert_env_equal "GH_OST_OLD_TABLE_NAME" "_gh_ost_test_del"
    assert_env_equal "GH_OST_TARGET_TABLE_NAME" "gh_ost_test"
    assert_env_equal "GH_OST_MOVE_TABLES" "true"
    assert_env_equal "GH_OST_REVERT" "false"
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