/**
 *
 */
package com.xasecure.common;


public interface MyCallBack {
    /**
     * Make sure to add @Transactional annotation to the implementation method.
     *
     * @Override
     * @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
     */
    public Object process(Object data);
}
