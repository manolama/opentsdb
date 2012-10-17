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

CURATOR_VERSION := 1.1.15
CURATOR_BASE_URL := http://www.euphoriaaudio.com/downloads

CURATOR_CLIENT := third_party/curator/curator-client-$(CURATOR_VERSION).jar
$(CURATOR_CLIENT): $(CURATOR_CLIENT).md5
	set dummy "$(CURATOR_BASE_URL)" "$(CURATOR_CLIENT)"; shift; $(FETCH_DEPENDENCY)

CURATOR_FRAMEWORK := third_party/curator/curator-framework-$(CURATOR_VERSION).jar
$(CURATOR_FRAMEWORK): $(CURATOR_FRAMEWORK).md5
	set dummy "$(CURATOR_BASE_URL)" "$(CURATOR_FRAMEWORK)"; shift; $(FETCH_DEPENDENCY)

CURATOR_RECIPES := third_party/curator/curator-recipes-$(CURATOR_VERSION).jar
$(CURATOR_RECIPES): $(CURATOR_RECIPES).md5
	set dummy "$(CURATOR_BASE_URL)" "$(CURATOR_RECIPES)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(CURATOR_CLIENT) $(CURATOR_FRAMEWORK) $(CURATOR_RECIPIES)
