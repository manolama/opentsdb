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

CASSANDRA_VERSION := 1.1.6

CASSANDRA_CORE_VERSION = $(CASSANDRA_VERSION)
CASSANDRA_CORE := third_party/cassandra/apache-cassandra-$(CASSANDRA_VERSION).jar
CASSANDRA_CORE_BASE_URL := http://www.euphoriaaudio.com/downloads

$(CASSANDRA_CORE): $(CASSANDRA_CORE).md5
	set dummy "$(CASSANDRA_CORE_BASE_URL)" "$(CASSANDRA_CORE)"; shift; $(FETCH_DEPENDENCY)

CASSANDRA_CLIENT_VERSION = $(CASSANDRA_VERSION)
CASSANDRA_CLIENT := third_party/cassandra/apache-cassandra-thrift-$(CASSANDRA_CLIENT_VERSION).jar
CASSANDRA_CLIENT_BASE_URL := http://www.euphoriaaudio.com/downloads

$(CASSANDRA_CLIENT): $(CASSANDRA_CLIENT).md5
	set dummy "$(CASSANDRA_CLIENT_BASE_URL)" "$(CASSANDRA_CLIENT)"; shift; $(FETCH_DEPENDENCY)


CASSANDRA_THRIFT_VERSION := 0.7.0

CASSANDRA_THRIFT := third_party/cassandra/libthrift-$(CASSANDRA_THRIFT_VERSION).jar
CASSANDRA_THRIFT_BASE_URL := http://www.euphoriaaudio.com/downloads

$(CASSANDRA_THRIFT): $(CASSANDRA_THRIFT).md5
	set dummy "$(CASSANDRA_THRIFT_BASE_URL)" "$(CASSANDRA_THRIFT)"; shift; $(FETCH_DEPENDENCY)


THIRD_PARTY += $(CASSANDRA_CORE) $(CASSANDRA_CLIENT) $(CASSANDRA_THRIFT)
