# Copyright (C) 2015 The OpenTSDB Authors.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 2.1 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

ASYNCBIGTABLE_VERSION := 0.2.0-20151027.224128-1
ASYNCBIGTABLE := third_party/asyncbigtable/asyncbigtable-$(ASYNCBIGTABLE_VERSION).jar
ASYNCBIGTABLE_BASE_URL := https://oss.sonatype.org/content/repositories/snapshots/com/pythian/opentsdb/asyncbigtable/0.2.0-SNAPSHOT/

$(ASYNCBIGTABLE): $(ASYNCBIGTABLE).md5
	set dummy "$(ASYNCBIGTABLE_BASE_URL)" "$(ASYNCBIGTABLE)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(ASYNCBIGTABLE)

NETTY4_VERSION := 4.1.0.Beta5
NETTY4 := third_party/netty/netty-all-$(NETTY4_VERSION).jar
NETTY4_BASE_URL := http://central.maven.org/maven2/io/netty/netty-all/$(NETTY4_VERSION)

$(NETTY4): $(NETTY4).md5
	set dummy "$(NETTY4_BASE_URL)" "$(NETTY4)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(NETTY4)