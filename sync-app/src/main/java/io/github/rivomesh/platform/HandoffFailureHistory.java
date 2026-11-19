package io.github.rivomesh.platform;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Inactive copies of already authenticated original evidence. This directory has no signing key. */
final class HandoffFailureHistory {
    static final String DIRECTORY="historical-failures",AUTHORITY="authority.json";
    static final int MAX_RECORDS=64,MAX_AUTHORITY=4096,MAX_RECORD_BYTES=8*1024*1024;
    private static final String RECORD="[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\\.json";
    private static final String PREFIX=DIRECTORY+"/";

    private HandoffFailureHistory(){}

    static boolean record(String name){return name.matches(RECORD);}
    static boolean member(String name){return AUTHORITY.equals(name)||record(name);}
    static boolean artifact(String name){return name.startsWith(PREFIX)&&member(name.substring(PREFIX.length()));}
    static String relative(String member){return PREFIX+member;}
    static boolean bounded(int records){return records>=0&&records<=MAX_RECORDS;}
    static int fileCount(int records){return records==0?0:1+records;}

    static void prepare(Path directory)throws Exception{
        if(Files.notExists(directory,LinkOption.NOFOLLOW_LINKS))HandoffStaging.createDirectory(directory);
        HandoffStaging.secureDirectory(directory);
    }

    static List<Map<String,Object>> verify(Path root,Map<String,HandoffStaging.FileSeal> proofs,
                                          int records,long deadline)throws Exception{
        Set<String> names=new TreeSet<>();
        for(String key:proofs.keySet()){
            if(!key.startsWith(PREFIX))continue;
            if(!artifact(key))throw refused();
            names.add(key.substring(PREFIX.length()));
        }
        if(!bounded(records)||names.size()!=fileCount(records)||records>0&&!names.contains(AUTHORITY))
            throw refused();
        if(records==0)return List.of();
        Path directory=root.resolve(DIRECTORY);
        HandoffStaging.secureDirectory(directory);
        HandoffStaging.exactNames(directory,names);
        List<Map<String,Object>> catalog=new ArrayList<>(records);
        for(String name:names){
            boolean authority=AUTHORITY.equals(name);
            var file=proofs.get(relative(name));
            HandoffStaging.checkFile(directory.resolve(name),file,authority?MAX_AUTHORITY:MAX_RECORD_BYTES,deadline);
            if(!authority)catalog.add(Map.of("id",name.substring(0,36),"sha256",file.sha256(),"bytes",file.bytes()));
        }
        return catalog;
    }

    private static TaskService.Conflict refused(){return new TaskService.Conflict("HANDOFF_FAILURE_HISTORY_SCOPE_CHANGED");}
}
