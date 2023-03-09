#
# Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# !/usr/bin/python

import os
import re
import sys

# list all sub modules under `module`
def list_sub_modules(module, include_pattern, exclude_patterns = None):
    files = os.scandir(module)
    sub_modules = [f.name for f in files if f.is_dir()]
    if include_pattern is not None:
        sub_modules = [module for module in sub_modules if re.fullmatch(include_pattern, module) is not None]
    if exclude_patterns is not None:
        for exclude_pattern in exclude_patterns:
            sub_modules = [module for module in sub_modules if re.fullmatch(exclude_pattern, module) is None]
    sub_modules = [os.path.join(module, sub_module) for sub_module in sub_modules]
    return sub_modules


# all integration test modules
def get_integration_test_all_modules():
    v1_module = 'bitsail-test/bitsail-test-integration'
    test_module_v1 = list_sub_modules(v1_module,
                                      'bitsail-test-integration-.*',
                                      ['bitsail-test-integration-connector-legacy',
                                       'bitsail-test-integration-kudu'])  # kudu test cannot be applied to all platforms
    # print('v1 connector integration test modules:\n' + '\n'.join(test_module_v1) + '')

    legacy_module = 'bitsail-test/bitsail-test-integration/bitsail-test-integration-connector-legacy'
    test_module_legacy = list_sub_modules(legacy_module, 'bitsail-test-integration-.*-legacy')
    # print('legacy connector integration test modules:\n' + '\n'.join(test_module_legacy) + '')

    all_test_module = test_module_v1 + test_module_legacy
    all_test_module = ','.join(all_test_module)
    # print('all integration test modules: ' + all_test_module)
    return all_test_module

# v1 connector e2e test modules
def get_e2e_test_all_modules():
    v1_module = 'bitsail-test/bitsail-test-end-to-end/bitsail-test-e2e-connector-v1'
    test_module_v1 = list_sub_modules(v1_module, 'bitsail-test-e2e-connector-v1-.*')
    test_module_v1 = ','.join(test_module_v1)
    # print('all e2e test modules: ' + test_module_v1)
    return test_module_v1


if __name__ == "__main__":
    type = sys.argv[1]
    if type == 'integration':
        print(get_integration_test_all_modules())
    elif type == 'e2e':
        print(get_e2e_test_all_modules())
    else:
        print('Wrong argument!')
        print('Please specify one of the test types: "integration" or "e2e"')
        sys.exit(1)