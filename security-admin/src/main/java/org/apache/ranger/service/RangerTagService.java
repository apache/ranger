package org.apache.ranger.service;

import org.apache.ranger.common.SearchField;
import org.apache.ranger.entity.XXTag;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.stereotype.Service;

/**
 * Created by akulkarni on 8/19/15.
 */

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

}