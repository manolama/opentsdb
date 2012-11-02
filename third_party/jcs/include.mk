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

JCS_VERSION := 1.3
JCS := third_party/jcs/jcs-$(JCS_VERSION).jar
JCS_BASE_URL := http://www.euphoriaaudio.com/downloads

$(JCS): $(JCS).md5
	set dummy "$(JCS_BASE_URL)" "$(JCS)"; shift; $(FETCH_DEPENDENCY)

CONCURRENT := third_party/jcs/concurrent.jar
CONCURRENT_BASE_URL := http://www.euphoriaaudio.com/downloads

$(CONCURRENT): $(CONCURRENT).md5
	set dummy "$(CONCURRENT_BASE_URL)" "$(CONCURRENT)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JCS) $(CONCURRENT)
