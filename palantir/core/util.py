#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from dataclasses import fields

from typing import Any, Dict


def dataclass_from_dict(klass: Any, dikt: Dict[str, Any]):
    try:
        fieldtypes = {f.name: f.type for f in fields(klass)}
        return klass(**{f: dataclass_from_dict(fieldtypes[f], dikt[f]) for f in dikt})
    except TypeError:
        return dikt


def alias(ignore_case=False):
    class Alias:
        _aliases = {}

        def alias(self, *aliases: str):
            def _decorator(cls):
                for _alias in aliases:
                    if ignore_case:
                        self._aliases[_alias.lower()] = cls
                    else:
                        self._aliases[_alias] = cls
                return cls

            return _decorator

        def __getitem__(self, item):
            return self._aliases[item.lower() if ignore_case else item]

    return Alias()


def page_results(values_extractor, token_extractor, page_supplier, page_token=None):
    def process_page(page):
        values = values_extractor(page)
        next_page_token = token_extractor(page)

        for value in values:
            yield value

        if next_page_token is not None:
            for value in process_page(page_supplier(next_page_token)):
                yield value

    return process_page(page_supplier(page_token))
