package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.entity.XXTag;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.stereotype.Service;


@Service
public class RangerTagService extends RangerTagServiceBase<XXTag, RangerTag> {

	public RangerTagService() {
		searchFields.add(new SearchField(SearchFilter.TAG_ID, "obj.id", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.TAG_NAME, "obj.name", SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL));
	}

	@Override
	protected void validateForCreate(RangerTag vObj) {

	}

	@Override
	protected void validateForUpdate(RangerTag vObj, XXTag entityObj) {

	}

	public RangerTag getPopulatedViewObject(XXTag xObj) {
		return populateViewBean(xObj);
	}

	public RangerTag getTagByGuid(String guid) {
		RangerTag ret = null;

		XXTag xxTag = daoMgr.getXXTag().findByGuid(guid);
		
		if(xxTag != null) {
			ret = populateViewBean(xxTag);
		}

		return ret;
	}

	public List<RangerTag> getTagsByName(String name) {
		List<RangerTag> ret = new ArrayList<RangerTag>();

		List<XXTag> xxTags = daoMgr.getXXTag().findByName(name);
		
		if(CollectionUtils.isNotEmpty(xxTags)) {
			for(XXTag xxTag : xxTags) {
				RangerTag tag = populateViewBean(xxTag);

				ret.add(tag);
			}
		}

		return ret;
	}

	public List<RangerTag> getTagsForResourceId(Long resourceId) {
		List<RangerTag> ret = new ArrayList<RangerTag>();

		List<XXTag> xxTags = daoMgr.getXXTag().findForResourceId(resourceId);
		
		if(CollectionUtils.isNotEmpty(xxTags)) {
			for(XXTag xxTag : xxTags) {
				RangerTag tag = populateViewBean(xxTag);

				ret.add(tag);
			}
		}

		return ret;
	}

	public List<RangerTag> getTagsForResourceGuid(String resourceGuid) {
		List<RangerTag> ret = new ArrayList<RangerTag>();

		List<XXTag> xxTags = daoMgr.getXXTag().findForResourceGuid(resourceGuid);
		
		if(CollectionUtils.isNotEmpty(xxTags)) {
			for(XXTag xxTag : xxTags) {
				RangerTag tag = populateViewBean(xxTag);

				ret.add(tag);
			}
		}

		return ret;
	}

	public List<RangerTag> getTagsByServiceId(Long serviceId) {
		List<RangerTag> ret = new ArrayList<RangerTag>();

		List<XXTag> xxTags = daoMgr.getXXTag().findByServiceId(serviceId);
		
		if(CollectionUtils.isNotEmpty(xxTags)) {
			for(XXTag xxTag : xxTags) {
				RangerTag tag = populateViewBean(xxTag);

				ret.add(tag);
			}
		}

		return ret;
	}
}