package com.xasecure.admin.client.datatype;

import java.io.IOException;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.mortbay.log.Log;

import com.sun.jersey.api.client.ClientResponse;
import com.xasecure.authorization.utils.StringUtil;


@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RESTResponse {
	private int           httpStatusCode;
	private int           statusCode;
	private String        msgDesc;
	private List<Message> messageList;


	public int getHttpStatusCode() {
		return httpStatusCode;
	}

	public void setHttpStatusCode(int httpStatusCode) {
		this.httpStatusCode = httpStatusCode;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public String getMsgDesc() {
		return msgDesc;
	}

	public void setMsgDesc(String msgDesc) {
		this.msgDesc = msgDesc;
	}

	public List<Message> getMessageList() {
		return messageList;
	}

	public void setMessageList(List<Message> messageList) {
		this.messageList = messageList;
	}
	
	public String getMessage() {
		return StringUtil.isEmpty(msgDesc) ? ("HTTP " + httpStatusCode) : msgDesc;
	}

	public String toJson() {
		try {
			ObjectMapper om = new ObjectMapper();

			return om.writeValueAsString(this);
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return "";
	}

	public static RESTResponse fromClientResponse(ClientResponse response) {
		RESTResponse ret = null;

		String jsonString = response == null ? null : response.getEntity(String.class);
		int    httpStatus = response == null ? 0 : response.getStatus();

		if(! StringUtil.isEmpty(jsonString)) {
			ret = RESTResponse.fromJson(jsonString);
		}

		if(ret == null) {
			ret = new RESTResponse();
		}

		ret.setHttpStatusCode(httpStatus);

		return ret;
	}

	public static RESTResponse fromJson(String jsonString) {
		try {
			ObjectMapper om = new ObjectMapper();

			return om.readValue(jsonString, RESTResponse.class);
		} catch (Exception e) {
			Log.warn("fromJson() failed!", e);
		}

		return null;
	}

	@Override
	public String toString() {
		return toJson();
	}

	@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
	@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Message {
		private String name;
		private String rbKey;
		private String message;
		private Long   objectId;
		private String fieldName;

		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getRbKey() {
			return rbKey;
		}
		public void setRbKey(String rbKey) {
			this.rbKey = rbKey;
		}
		public String getMessage() {
			return message;
		}
		public void setMessage(String message) {
			this.message = message;
		}
		public Long getObjectId() {
			return objectId;
		}
		public void setObjectId(Long objectId) {
			this.objectId = objectId;
		}
		public String getFieldName() {
			return fieldName;
		}
		public void setFieldName(String fieldName) {
			this.fieldName = fieldName;
		}

		public String toJson() {
			try {
				ObjectMapper om = new ObjectMapper();

				return om.writeValueAsString(this);
			} catch (JsonGenerationException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			return "";
		}

		public static RESTResponse fromJson(String jsonString) {
			try {
				ObjectMapper om = new ObjectMapper();

				return om.readValue(jsonString, RESTResponse.class);
			} catch (Exception e) {
				// ignore
			}
			
			return null;
		}

		@Override
		public String toString() {
			return toJson();
		}
	}
}
