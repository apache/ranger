package org.apache.ranger.service;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class RangerBaseModelService<T extends XXDBBase, V extends RangerBaseModelObject> {
	private static final Log LOG = LogFactory
			.getLog(RangerBaseModelService.class);

	@Autowired
	protected RangerDaoManager daoMgr;

	@Autowired
	protected StringUtil stringUtil;

	@Autowired
	protected RESTErrorUtil restErrorUtil;

	public static final int OPERATION_CREATE_CONTEXT = 1;
	public static final int OPERATION_UPDATE_CONTEXT = 2;

	protected Class<T> tEntityClass;
	protected Class<V> tViewClass;

	BaseDao<T> entityDao;

	@SuppressWarnings("unchecked")
	public RangerBaseModelService() {
		Class klass = getClass();
		ParameterizedType genericSuperclass = (ParameterizedType) klass
				.getGenericSuperclass();
		TypeVariable<Class<?>> var[] = klass.getTypeParameters();

		if (genericSuperclass.getActualTypeArguments()[0] instanceof Class) {
			tEntityClass = (Class<T>) genericSuperclass
					.getActualTypeArguments()[0];
			tViewClass = (Class<V>) genericSuperclass.getActualTypeArguments()[1];
		} else if (var.length > 0) {
			tEntityClass = (Class<T>) var[0].getBounds()[0];
			tViewClass = (Class<V>) var[1].getBounds()[0];
		} else {
			LOG.fatal("Cannot find class for template", new Throwable());
		}
	}

	protected abstract T mapViewToEntityBean(V viewBean, T t,
			int OPERATION_CONTEXT);

	protected abstract V mapEntityToViewBean(V viewBean, T t);

	protected T createEntityObject() {
		try {
			return tEntityClass.newInstance();
		} catch (Throwable e) {
			LOG.error("Error instantiating entity class. tEntityClass="
					+ tEntityClass.toString(), e);
		}
		return null;
	}

	protected V createViewObject() {
		try {
			return tViewClass.newInstance();
		} catch (Throwable e) {
			LOG.error("Error instantiating view class. tViewClass="
					+ tViewClass.toString(), e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	protected BaseDao<T> getDao() {
		if (entityDao == null) {
			entityDao = (BaseDao<T>) daoMgr.getDaoForClassName(tEntityClass
					.getSimpleName());

		}
		return entityDao;
	}

	protected V populateViewBean(T entityObj) {
		V vObj = createViewObject();
		vObj.setId(entityObj.getId());
		vObj.setCreateTime(entityObj.getCreateTime());
		vObj.setUpdateTime(entityObj.getUpdateTime());

		if (entityObj.getAddedByUserId() != null) {
			XXPortalUser tUser = daoMgr.getXXPortalUser().getById(
					entityObj.getUpdatedByUserId());
			if(tUser == null) {
				// nothing to do
			} else if (!stringUtil.isEmpty(tUser.getPublicScreenName())) {
				vObj.setCreatedBy(tUser.getPublicScreenName());
			} else {
				if (!stringUtil.isEmpty(tUser.getFirstName())) {
					if (!stringUtil.isEmpty(tUser.getLastName())) {
						vObj.setCreatedBy(tUser.getFirstName() + " "
								+ tUser.getLastName());
					} else {
						vObj.setCreatedBy(tUser.getFirstName());
					}
				} else {
					vObj.setCreatedBy(tUser.getLoginId());
				}
			}
		}
		if (entityObj.getUpdatedByUserId() != null) {
			XXPortalUser tUser = daoMgr.getXXPortalUser().getById(
					entityObj.getUpdatedByUserId());
			if(tUser == null) {
				// nothing to do
			} else if (!stringUtil.isEmpty(tUser.getPublicScreenName())) {
				vObj.setUpdatedBy(tUser.getPublicScreenName());
			} else {
				if (!stringUtil.isEmpty(tUser.getFirstName())) {
					if (!stringUtil.isEmpty(tUser.getLastName())) {
						vObj.setUpdatedBy(tUser.getFirstName() + " "
								+ tUser.getLastName());
					} else {
						vObj.setUpdatedBy(tUser.getFirstName());
					}
				} else {
					vObj.setUpdatedBy(tUser.getLoginId());
				}
			}
		}
		
		return mapEntityToViewBean(vObj, entityObj);
	}

	protected T populateEntityBean(V vObj, int operationContext) {
		T entityObj;

		Date createTime = null;
		Date updTime = null;
		Long createdById = null;
		Long updById = null;

		if (operationContext == OPERATION_CREATE_CONTEXT) {
			entityObj = createEntityObject();

			createTime = DateUtil.getUTCDate();
			updTime = DateUtil.getUTCDate();
			createdById = ContextUtil.getCurrentUserId();
			updById = ContextUtil.getCurrentUserId();
		} else if (operationContext == OPERATION_UPDATE_CONTEXT) {
			entityObj = getDao().getById(vObj.getId());

			if (entityObj == null) {
				throw restErrorUtil.createRESTException(
						"No Object found to update.",
						MessageEnums.DATA_NOT_FOUND);
			}

			createTime = entityObj.getCreateTime();
			if (createTime == null) {
				createTime = DateUtil.getUTCDate();
			}
			updTime = DateUtil.getUTCDate();

			createdById = entityObj.getAddedByUserId();
			if (createdById == null) {
				createdById = ContextUtil.getCurrentUserId();
			}
			updById = ContextUtil.getCurrentUserId();
		} else {
			throw restErrorUtil.createRESTException(
					"Error while populating EntityBean",
					MessageEnums.INVALID_INPUT_DATA);
		}
		entityObj.setAddedByUserId(createdById);
		entityObj.setUpdatedByUserId(updById);
		entityObj.setCreateTime(createTime);
		entityObj.setUpdateTime(updTime);

		return mapViewToEntityBean(vObj, entityObj, operationContext);
	}

	protected abstract void validateForCreate(V vObj);

	protected abstract void validateForUpdate(V vObj, T entityObj);

	public T preCreate(V vObj) {
		validateForCreate(vObj);
		return populateEntityBean(vObj, OPERATION_CREATE_CONTEXT);
	}

	public V postCreate(T xObj) {
		return populateViewBean(xObj);
	}

	public V create(V vObj) {
		T resource = preCreate(vObj);
		resource = getDao().create(resource);
		vObj = postCreate(resource);
		return vObj;
	}

	public V read(Long id) {
		T resource = getDao().getById(id);
		if (resource == null) {
			throw restErrorUtil.createRESTException(tViewClass.getName()
					+ " :Data Not Found for given Id",
					MessageEnums.DATA_NOT_FOUND, id, null,
					"readResource : No Object found with given id.");
		}
		return populateViewBean(resource);
	}
	
	public V update(V viewBaseBean) {
		T resource = preUpdate(viewBaseBean);
		resource = getDao().update(resource);
		V viewBean = postUpdate(resource);
		return viewBean;
	}

	public V postUpdate(T resource) {
		return populateViewBean(resource);
	}

	public T preUpdate(V viewBaseBean) {
		T resource = getDao().getById(viewBaseBean.getId());
		if (resource == null) {
			throw restErrorUtil.createRESTException(tEntityClass.getSimpleName()
					+ " not found", MessageEnums.DATA_NOT_FOUND,
					viewBaseBean.getId(), null, "preUpdate: id not found.");
		}
		validateForUpdate(viewBaseBean, resource);
		return populateEntityBean(viewBaseBean, OPERATION_UPDATE_CONTEXT);
	}
	
	public boolean delete(V vObj) {
		boolean result = false;
		Long id = vObj.getId();
		T resource = preDelete(id);
		if (resource == null) {
			throw restErrorUtil.createRESTException(
					tEntityClass.getSimpleName() + " not found",
					MessageEnums.DATA_NOT_FOUND, id, null,
					tEntityClass.getSimpleName() + ":" + id);
		}
		try {
			result = getDao().remove(resource);
		} catch (Exception e) {
			LOG.error("Error deleting " + tEntityClass.getSimpleName()
					+ ". Id=" + id, e);

			throw restErrorUtil.createRESTException(
					tEntityClass.getSimpleName() + " can't be deleted",
					MessageEnums.OPER_NOT_ALLOWED_FOR_STATE, id, null, "" + id
							+ ", error=" + e.getMessage());
		}
		return result;
	}

	protected T preDelete(Long id) {
		T resource = getDao().getById(id);
		if (resource == null) {
			// Return without error
			LOG.info("Delete ignored for non-existent Object, id=" + id);
		}
		return resource;
	}
	
}
