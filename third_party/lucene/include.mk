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

LUCENE_VERSION := 3.6.1
LUCENE := third_party/lucene/lucene-core-$(LUCENE_VERSION).jar
LUCENE_BASE_URL := http://www.euphoriaaudio.com/downloads

$(LUCENE): $(LUCENE).md5
	set dummy "$(LUCENE_BASE_URL)" "$(LUCENE)"; shift; $(FETCH_DEPENDENCY)

LUCENE_GROUPING := third_party/lucene/lucene-grouping-$(LUCENE_VERSION).jar

$(LUCENE_GROUPING): $(LUCENE_GROUPING).md5
	set dummy "$(LUCENE_BASE_URL)" "$(LUCENE_GROUPING)"; shift; $(FETCH_DEPENDENCY)

LUCENE_QUERIES := third_party/lucene/lucene-queries-$(LUCENE_VERSION).jar

$(LUCENE_QUERIES): $(LUCENE_QUERIES).md5
	set dummy "$(LUCENE_BASE_URL)" "$(LUCENE_QUERIES)"; shift; $(FETCH_DEPENDENCY)


THIRD_PARTY += $(LUCENE) $(LUCENE_GROUPING) $(LUCENE_QUERIES)
