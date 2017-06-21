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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the JSON Model that GeoServer uses to transfer information
 * regarding Layer Groups. This is used with the GeoServer REST API to create or
 * update Layer Groups.
 * 
 * @author Patrick.Doody
 *
 */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LayerGroupModel {
	
	private LayerGroup layerGroup = new LayerGroup();

	public LayerGroupModel() {
		// Needed for Jackson support.
	}

	public LayerGroup getLayerGroup() {
		return layerGroup;
	}

	public void setLayerGroup(LayerGroup layerGroup) {
		this.layerGroup = layerGroup;
	}

	@JsonInclude(Include.NON_NULL)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class LayerGroup {
		private String name;
		private String mode = "SINGLE";

		private Publishable publishables = new Publishable();
		private Styles styles = new Styles();

		public LayerGroup() {
			// Needed for Jackson support.
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Publishable getPublishables() {
			return publishables;
		}

		public void setPublishables(Publishable publishables) {
			this.publishables = publishables;
		}

		public Styles getStyles() {
			return styles;
		}

		public void setStyles(Styles styles) {
			this.styles = styles;
		}

		public String getMode() {
			return mode;
		}

		public void setMode(String mode) {
			this.mode = mode;
		}
	}

	@JsonInclude(Include.NON_NULL)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Publishable {
		@JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
		private List<GroupLayer> published = new ArrayList<GroupLayer>();

		public Publishable() {
			// Needed for Jackson support.
		}

		public List<GroupLayer> getPublished() {
			return published;
		}

		public void setPublished(List<GroupLayer> published) {
			this.published = published;
		}
	}

	@JsonInclude(Include.NON_NULL)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class GroupLayer {
		@JsonProperty(value = "@type")
		private String type = "layer";
		private String name;

		public GroupLayer() {
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

	@JsonInclude(Include.NON_NULL)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Styles {
		@JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
		private List<String> style = new ArrayList<>();

		public Styles() {
			// Needed for Jackson support.
		}

		public List<String> getStyle() {
			return style;
		}

		public void setStyle(List<String> style) {
			this.style = style;
		}
	}
}
