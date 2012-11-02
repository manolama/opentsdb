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

COMMONS_MATH_VERSION := 3-3.0

COMMONS_MATH = $(COMMONS_MATH_VERSION)
COMMONS_MATH := third_party/apache/commons-math$(COMMONS_MATH_VERSION).jar
COMMONS_MATH_BASE_URL := http://www.euphoriaaudio.com/downloads/

$(COMMONS_MATH): $(COMMONS_MATH).md5
	set dummy "$(COMMONS_MATH_BASE_URL)" "$(COMMONS_MATH)"; shift; $(FETCH_DEPENDENCY)

COMMONS_LOGGING_VERSION := 1.1.1

COMMONS_LOGGING = $(COMMONS_LOGGING_VERSION)
COMMONS_LOGGING := third_party/apache/commons-logging-$(COMMONS_LOGGING_VERSION).jar
COMMONS_LOGGING_BASE_URL := http://www.euphoriaaudio.com/downloads/

$(COMMONS_LOGGING): $(COMMONS_LOGGING).md5
	set dummy "$(COMMONS_LOGGING_BASE_URL)" "$(COMMONS_LOGGING)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(COMMONS_MATH) $(COMMONS_LOGGING)