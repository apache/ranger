package org.apache.ranger.kms.dao;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;

public class DaoManager extends DaoManagerBase {

	@PersistenceContext
	private EntityManagerFactory emf;

	static ThreadLocal<EntityManager> sEntityManager;

	public void setEntityManagerFactory(EntityManagerFactory emf) {
		this.emf = emf;
		sEntityManager = new ThreadLocal<EntityManager>();
	}

	@Override
	public EntityManager getEntityManager() {
		EntityManager em = null;

		if(sEntityManager != null) {
			em = sEntityManager.get();

			if(em == null && this.emf != null) {
				em = this.emf.createEntityManager();

				sEntityManager.set(em);
			}
		}

		return em;
	}
}