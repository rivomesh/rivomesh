package io.github.rivomesh.platform;

import java.util.Map;

/** Transport-independent task boundary; IAM/session/audit APIs remain separate. */
interface TaskControlPlane {
    default boolean localOnly(){return true;}
    Map<String,Object> invoke(TaskRequest request,AccessControl.Principal actor)throws Exception;
    default Map<String,Object> ownerQuery(TaskRequest request,AccessControl.Principal actor)throws Exception{return invoke(request,actor);}
}
