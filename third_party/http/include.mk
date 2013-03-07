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

HTTP_CORE_VERSION := 4.2.2

HTTP_CORE = $(HTTP_CORE_VERSION)
HTTP_CORE := third_party/http/httpcore-$(HTTP_CORE_VERSION).jar
HTTP_CORE_BASE_URL := http://www.euphoriaaudio.com/downloads/

$(HTTP_CORE): $(HTTP_CORE).md5
	set dummy "$(HTTP_CORE_BASE_URL)" "$(HTTP_CORE)"; shift; $(FETCH_DEPENDENCY)

HTTP_CLIENT_VERSION := 4.2.3

HTTP_CLIENT = $(HTTP_CLIENT_VERSION)
HTTP_CLIENT := third_party/http/httpclient-$(HTTP_CLIENT_VERSION).jar
HTTP_CLIENT_BASE_URL := http://www.euphoriaaudio.com/downloads/

$(HTTP_CLIENT): $(HTTP_CLIENT).md5
	set dummy "$(HTTP_CLIENT_BASE_URL)" "$(HTTP_CLIENT)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(HTTP_CORE) $(HTTP_CLIENT)