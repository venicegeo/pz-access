/**
 * Copyright 2016, RadiantBlue Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package access.deploy.geoserver;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

/**
 * Represents the XML Model that GeoServer uses to transfer information regarding Layer Groups. This is used with the
 * GeoServer REST API to create or update Layer Groups.
 * 
 * @author Patrick.Doody
 *
 */
public class LayerGroupModel2 {
	private LayerGroup2 layerGroup = new LayerGroup2();

	public LayerGroupModel2() {
		// Needed for Jackson support.
	}

	public LayerGroup2 getLayerGroup() {
		return layerGroup;
	}

	public void setLayerGroup(LayerGroup2 layerGroup) {
		this.layerGroup = layerGroup;
	}

	@JacksonXmlRootElement(localName = "layerGroup")
	public static class LayerGroup2 {
		@JacksonXmlProperty(isAttribute = false, localName = "name")
		private String name;
		private String mode = "SINGLE";

		@JacksonXmlElementWrapper(localName = "publishables")
		@JacksonXmlProperty(localName = "published")
		private List<GroupLayer2> published = new ArrayList<>();

		@JacksonXmlElementWrapper(localName = "styles")
		@JacksonXmlProperty(localName = "style")
		private List<String> style = new ArrayList<>();

		public LayerGroup2() {
			// Needed for Jackson support.
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getMode() {
			return mode;
		}

		public void setMode(String mode) {
			this.mode = mode;
		}

		public List<GroupLayer2> getPublished() {
			return published;
		}

		public void setPublished(List<GroupLayer2> published) {
			this.published = published;
		}

		public List<String> getStyle() {
			return style;
		}

		public void setStyle(List<String> style) {
			this.style = style;
		}
	}

	public static class GroupLayer2 {
		@JacksonXmlProperty(isAttribute = true)
		private String type = "layer";

		private String name;

		public GroupLayer2() {
			// Needed for Jackson support.
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
}