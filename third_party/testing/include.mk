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
#
ONTOPIA_VERSION := 5.2.1
ONTOPIA := third_party/testing/ontopia-engine-$(ONTOPIA_VERSION).jar
ONTOPIA_BASE_URL := http://www.euphoriaaudio.com/downloads

$(ONTOPIA): $(ONTOPIA).md5
	set dummy "$(ONTOPIA_BASE_URL)" "$(ONTOPIA)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(ONTOPIA)

RABBITMQ_VERSION := client
RABBITMQ := third_party/testing/rabbitmq-$(RABBITMQ_VERSION).jar
RABBITMQ_BASE_URL := http://www.euphoriaaudio.com/downloads

$(RABBITMQ): $(RABBITMQ).md5
	set dummy "$(RABBITMQ_BASE_URL)" "$(RABBITMQ)"; shift; $(FETCH_DEPENDENCY)
  
THIRD_PARTY += $(RABBITMQ)