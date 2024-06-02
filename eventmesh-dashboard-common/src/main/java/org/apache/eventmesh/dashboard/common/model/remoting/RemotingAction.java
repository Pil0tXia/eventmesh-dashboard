package org.apache.eventmesh.dashboard.common.model.remoting;

import org.apache.eventmesh.dashboard.common.enums.RemotingType;

public @interface RemotingAction {

    boolean support();

    RemotingType substitution();

    /**
     * A fallback mechanism for the substitution method. Can be used in RemotingServiceHandler. Not used for now.
     */
    RemotingType retrySubstitution() default RemotingType.STORAGE;
}
