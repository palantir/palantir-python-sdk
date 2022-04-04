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

from expects import expect, raise_error, equal, be_none

from palantir.core.types import ResourceIdentifier


class TestResourceIdentifier:
    def test_from_string(self):
        raw = "ri.foundry.main.dataset.00000000-0000-0000-0000-000000000000"
        expected = ResourceIdentifier(
            "foundry", "main", "dataset", "00000000-0000-0000-0000-000000000000"
        )
        expect(ResourceIdentifier.from_string(raw)).to(equal(expected))

    def test_from_string_with_invalid_rid(self):
        raw = "ri.foundry.dataset.0"
        expect(lambda: ResourceIdentifier.from_string(raw)).to(
            raise_error(ValueError, "value could not be parsed as a ResourceIdentifier")
        )

    def test_try_parse(self):
        raw = "ri.foundry.main.dataset.00000000-0000-0000-0000-000000000000"
        expected = ResourceIdentifier(
            "foundry", "main", "dataset", "00000000-0000-0000-0000-000000000000"
        )
        expect(ResourceIdentifier.try_parse(raw)).to(equal(expected))

    def test_try_parse_with_invalid_rid(self):
        raw = "ri.foundry.dataset.0"
        expect(ResourceIdentifier.try_parse(raw)).to(be_none)

    def test_str(self):
        raw = "ri.foundry.main.dataset.00000000-0000-0000-0000-000000000000"
        expect(str(ResourceIdentifier.from_string(raw))).to(equal(raw))
