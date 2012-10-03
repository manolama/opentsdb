# Copyright (C) 2011  The OpenTSDB Authors.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

GUAVA_VERSION := 13.0.1

GUAVA_CORE_VERSION = $(GUAVA_VERSION)
GUAVA_CORE := third_party/guava/guava-$(GUAVA_CORE_VERSION).jar
GUAVA_CORE_BASE_URL := http://www.euphoriaaudio.com/downloads/

$(GUAVA_CORE): $(GUAVA_CORE).md5
	set dummy "$(GUAVA_CORE_BASE_URL)" "$(GUAVA_CORE)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(GUAVA_CORE) 