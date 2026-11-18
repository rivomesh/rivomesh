package io.github.rivomesh.platform;

import io.github.rivomesh.cdc.PipelineFailure.Phase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class WorkerDiagnostics {
    enum Event {
        CAPTURE_READY, CAPTURE_FAILURE, RETRY, BACKPRESSURE, BACKPRESSURE_CLEARED,
        TARGET_PAUSED, TARGET_RETRYING, TARGET_RETRY_CLEARED;

        boolean target(){return this==TARGET_PAUSED||this==TARGET_RETRYING||this==TARGET_RETRY_CLEARED;}
    }

    private static final int SEGMENT_BYTES=64*1024;
    private static final String EVENT_ID="[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}";
    private static final String SUBSCRIPTION_ID="[A-Za-z0-9][A-Za-z0-9._-]{0,127}";
    private static final Pattern TARGET_FAILURE=Pattern.compile(
            "(TARGET_PAUSED|TARGET_RETRYING) ("+SUBSCRIPTION_ID+") [A-Z][A-Z0-9_]{0,127}");
    private static final Pattern TARGET_RECOVERY=Pattern.compile("(TARGET_RETRY_CLEARED) ("+SUBSCRIPTION_ID+")");

    private WorkerDiagnostics(){}

    static void validatePage(String cursor,int limit,String subscription){
        if(cursor==null||(!cursor.isEmpty()&&!cursor.matches(EVENT_ID))||limit<1||limit>25
                ||subscription!=null&&!subscription.matches(SUBSCRIPTION_ID))
            throw new IllegalArgumentException("DIAGNOSTICS_PAGE_INVALID");
    }

    static synchronized void append(Path rawLog,Event event,Phase phase)throws IOException{
        append(rawLog,event,phase==null?"-":phase.name());
    }

    static synchronized void observeTarget(Path rawLog,String message)throws IOException{
        Matcher fields=message.startsWith("TARGET_RETRY_CLEARED ")
                ?TARGET_RECOVERY.matcher(message):TARGET_FAILURE.matcher(message);
        if(fields.matches())append(rawLog,Event.valueOf(fields.group(1)),fields.group(2));
    }

    private static void append(Path rawLog,Event event,String detail)throws IOException{
        byte[] line=(UUID.randomUUID()+" "+Instant.now()+" "+event.name()+" "+detail+"\n")
                .getBytes(StandardCharsets.US_ASCII);
        Path current=segment(rawLog,event.target(),false);
        if(Files.isRegularFile(current,LinkOption.NOFOLLOW_LINKS)&&Files.size(current)+line.length>SEGMENT_BYTES)
            Files.move(current,segment(rawLog,event.target(),true),StandardCopyOption.REPLACE_EXISTING);
        try(OutputStream out=Files.newOutputStream(current,StandardOpenOption.CREATE,StandardOpenOption.APPEND,
                LinkOption.NOFOLLOW_LINKS)){
            PrivateFiles.ownerOnly(current);
            out.write(line);
        }
    }

    static synchronized Map<String,Object> page(Path rawLog,String cursor,int limit,String subscription)throws IOException{
        validatePage(cursor,limit,subscription);
        List<Map<String,String>> events=new ArrayList<>();
        read(segment(rawLog,subscription!=null,true),subscription,events);
        read(segment(rawLog,subscription!=null,false),subscription,events);
        Collections.reverse(events);
        int start=0;
        if(!cursor.isEmpty()){
            while(start<events.size()&&!cursor.equals(events.get(start).get("id")))start++;
            if(start==events.size())throw new TaskService.Conflict("DIAGNOSTICS_CURSOR_EXPIRED");
            start++;
        }
        int end=Math.min(start+limit,events.size());
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("events",new ArrayList<>(events.subList(start,end)));
        result.put("nextCursor",end<events.size()?events.get(end-1).get("id"):null);
        result.put("observedAt",Instant.now().toString());
        if(subscription!=null)result.put("subscriptionId",subscription);
        return result;
    }

    private static Path segment(Path log,boolean target,boolean previous){
        return log.resolveSibling(log.getFileName()+(target?".target-events":".events")+(previous?".1":""));
    }

    private static void read(Path path,String subscription,List<Map<String,String>> events)throws IOException{
        try(InputStream input=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS);
            ByteArrayOutputStream buffer=new ByteArrayOutputStream()){
            byte[] bytes=new byte[4096];
            for(int n;(n=input.read(bytes))!=-1;){
                if(buffer.size()+n>SEGMENT_BYTES)throw new IOException("DIAGNOSTICS_SEGMENT_LIMIT");
                buffer.write(bytes,0,n);
            }
            String text=new String(buffer.toByteArray(),StandardCharsets.US_ASCII);
            for(String line:text.split("\n")){
                if(line.length()>224)continue;
                String[] fields=line.split(" ",-1);
                if(fields.length!=4||!fields[0].matches(EVENT_ID))continue;
                try{
                    Map<String,String> item=new LinkedHashMap<>();
                    item.put("id",fields[0]);
                    item.put("at",Instant.parse(fields[1]).toString());
                    Event event=Event.valueOf(fields[2]);
                    if(event.target()!=(subscription!=null))continue;
                    item.put("event",event.name());
                    if(event.target()){
                        if(!subscription.equals(fields[3]))continue;
                    }else if(event==Event.CAPTURE_FAILURE)item.put("phase",Phase.valueOf(fields[3]).name());
                    else if(!"-".equals(fields[3]))continue;
                    events.add(item);
                }catch(IllegalArgumentException|java.time.format.DateTimeParseException rejected){
                    // Return only records that match the structured diagnostic format.
                }
            }
        }catch(NoSuchFileException ignored){
            // A worker has not emitted an event for this segment yet.
        }
    }
}
