import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {readFile} from 'node:fs/promises';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';

export async function waitFor(test,label,seconds=15){
  const deadline=Date.now()+seconds*1000;
  let lastError;
  while(Date.now()<deadline){
    try{const result=await test();if(result)return result;}catch(error){lastError=error;}
    await delay(100);
  }
  throw new Error('Timed out: '+label,{cause:lastError});
}

export async function launchChrome(binary,profile){
  const child=spawn(binary,['--headless','--remote-debugging-port=0',
    '--user-data-dir='+profile,'--no-first-run','about:blank'],{stdio:'ignore'});
  let failed;
  child.once('error',error=>{failed=error;});
  let socket;
  const pending=new Map();
  let sequence=0;
  async function close(){
    if(socket?.readyState===WebSocket.OPEN||socket?.readyState===WebSocket.CONNECTING)socket.close();
    if(!child.pid||child.exitCode!==null||child.signalCode!==null)return;
    child.kill('SIGTERM');
    await Promise.race([once(child,'exit').catch(()=>{}),delay(2000)]);
    if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await once(child,'exit');}
  }
  try{
    const portFile=await waitFor(()=>failed||readFile(join(profile,'DevToolsActivePort'),'utf8'),'Chrome DevTools');
    if(failed)throw failed;
    const debug='http://127.0.0.1:'+portFile.split('\n')[0];
    const {Browser:version}=await (await fetch(debug+'/json/version')).json();
    const pages=await (await fetch(debug+'/json/list')).json();
    socket=new WebSocket(pages.find(page=>page.type==='page').webSocketDebuggerUrl);
    await once(socket,'open');
    socket.onmessage=message=>{
      const event=JSON.parse(message.data),request=pending.get(event.id);
      if(!request)return;
      pending.delete(event.id);clearTimeout(request.timeout);
      if(event.error)request.reject(new Error(event.error.message));else request.done(event.result);
    };
    socket.onclose=()=>{
      for(const request of pending.values()){clearTimeout(request.timeout);request.reject(new Error('Chrome disconnected'));}
      pending.clear();
    };
    function cdp(method,params={}){
      const id=++sequence;
      return new Promise((done,reject)=>{
        const timeout=setTimeout(()=>{pending.delete(id);reject(new Error(method+' timed out'));},10000);
        pending.set(id,{done,reject,timeout});
        socket.send(JSON.stringify({id,method,params}));
      });
    }
    async function evaluate(expression){
      const {result,exceptionDetails}=await cdp('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});
      assert(!exceptionDetails,exceptionDetails?.text);
      return result.value;
    }
    async function key(name,code){
      const params={key:name,code:name,windowsVirtualKeyCode:code};
      await cdp('Input.dispatchKeyEvent',name==='Enter'?{...params,type:'keyDown',text:'\r'}:{...params,type:'rawKeyDown'});
      await cdp('Input.dispatchKeyEvent',{...params,type:'keyUp'});
    }
    return{cdp,evaluate,key,close,version};
  }catch(error){await close();throw error;}
}
