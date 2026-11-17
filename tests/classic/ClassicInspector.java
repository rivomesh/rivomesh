package io.github.rivomesh.acceptance;

import io.github.rivomesh.cdc.*;
import io.github.rivomesh.transport.classic.ClassicTransport;
import io.github.rivomesh.classic.internal.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;

/** Real consumer fixture: persist its received batch before acknowledging, preserving duplicates for test dedup. */
public final class ClassicInspector {
    public static void main(String[] args)throws Exception{
        ActiveMQConnectionFactory factory=new ActiveMQConnectionFactory("tcp://classic:61616?connectionTimeout=2000&soTimeout=3000");factory.setWatchTopicAdvisories(false);factory.setConnectResponseTimeout(2000);factory.setSendTimeout(3000);factory.setCloseTimeout(3000);factory.getPrefetchPolicy().setQueuePrefetch(1);
        Connection connection=factory.createConnection("consumer","Consumer@Test123");Session session=null;MessageConsumer consumer=null;
        try{connection.start();session=connection.createSession(false,Session.CLIENT_ACKNOWLEDGE);consumer=session.createConsumer(session.createQueue(args[0]),ClassicTransport.EVENT_SELECTOR);List<MessageEvent> events=new ArrayList<>();Message last=null;int total=0;
            for(int count=0;count<256;count++){Message raw=consumer.receive(count==0?500:30);if(raw==null)break;if(!(raw instanceof BytesMessage))throw new AssertionError("Non-event body");BytesMessage message=(BytesMessage)raw;long size=message.getBodyLength();if(size<0||size>1024*1024||size>16*1024*1024-total)throw new AssertionError("Fixture batch limit");byte[] bytes=new byte[(int)size];int read=message.readBytes(bytes);if(read!=bytes.length)throw new AssertionError("Truncated native body");String body=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();MessageEvent event=Codec.decode(body,MessageEvent.class);if(!event.id.equals(message.getStringProperty("rivomesh_event_id")))throw new AssertionError("Event identity mismatch");events.add(event);total+=size;last=raw;}
            Path output=Paths.get(args[1]),temporary=output.resolveSibling(output.getFileName()+".tmp");byte[] body=Codec.encode(events).getBytes(StandardCharsets.UTF_8);try(FileChannel channel=FileChannel.open(temporary,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)){ByteBuffer data=ByteBuffer.wrap(body);while(data.hasRemaining())channel.write(data);channel.force(true);}Files.move(temporary,output,StandardCopyOption.ATOMIC_MOVE);if(last!=null)last.acknowledge();
        }finally{if(consumer!=null)consumer.close();if(session!=null)session.close();connection.close();}
    }
}
