/** Persistent original-Aave interest normalization; one JSON request per line.
 * --artifact <kernels.json> is mandatory. --runtime-root is optional when the
 * pinned runtime is installed beside this worker. No RPC or filesystem writes.
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
const options=new Map();
for(let i=2;i<process.argv.length;i+=2){if(!process.argv[i].startsWith('--')||!process.argv[i+1])throw Error('Expected --artifact path [--runtime-root path]');options.set(process.argv[i],process.argv[i+1]);}
if(!options.has('--artifact'))throw Error('Explicit --artifact path is required');
const require=options.has('--runtime-root')?createRequire(path.resolve(options.get('--runtime-root'),'package.json')):createRequire(import.meta.url);
const load=async name=>import(pathToFileURL(require.resolve(name)).href);
const [{createEVM},{createBlock},{createAddressFromString,Account},{Interface,getBytes,hexlify,keccak256}]=await Promise.all(['@ethereumjs/evm','@ethereumjs/block','@ethereumjs/util','ethers'].map(load));
const artifact=JSON.parse(await fs.readFile(options.get('--artifact'),'utf8'));
if(artifact.format!==1||!Array.isArray(artifact.profiles))throw Error('Unsupported interest artifact');
const runtimes=new Map(),address=createAddressFromString('0x0000000000000000000000000000000000012345');
async function runtime(key){
 if(!runtimes.has(key)){
  const k=artifact.kernels[key];if(!k||keccak256(k.code)!==key)throw Error('Interest kernel identity differs');
  const evm=await createEVM();await evm.stateManager.putAccount(address,new Account());await evm.stateManager.putCode(address,getBytes(k.code));
  runtimes.set(key,{evm,iface:new Interface(k.abi),lastTimestamp:null,block:null});
 }
 return runtimes.get(key);
}
function integer(value,name,bits){
 if(typeof value==='number'&&!Number.isSafeInteger(value))throw Error('Unsafe integer '+name);
 if(!/^[0-9]+$/.test(String(value)))throw Error('Unsigned integer required: '+name);
 const n=BigInt(value);if(n>=1n<<BigInt(bits))throw Error('Out of range '+name);return n;
}
async function normalize(p){
 if(p.operation!=='normalize')throw Error('Unsupported interest operation');
 const block=Number(integer(p.block,'block',53));
 const profile=artifact.profiles.find(x=>x.from_block<=block&&block<x.to_block);
 if(!profile)throw Error('No verified historical interest profile for block '+block);
 if(!Array.isArray(p.rows))throw Error('rows must be an array');
 const rt=await runtime(profile.kernel),rows=[];
 for(const row of p.rows){
  const fields=['li','lr','vi','vr','updated'];
  const args=fields.map(name=>integer(row[name],name,name==='updated'?40:128));
  const timestamp=integer(row.timestamp,'timestamp',40);
  if(timestamp<args[4])throw Error('timestamp precedes reserve updated timestamp');
  if(rt.lastTimestamp!==timestamp){rt.block=createBlock({header:{timestamp}});rt.lastTimestamp=timestamp;}
  const response=await rt.evm.runCall({to:address,caller:address,data:getBytes(rt.iface.encodeFunctionData('normalize',args)),gasLimit:1000000n,block:rt.block});
  if(response.execResult.exceptionError)throw Error('Original Aave interest reverted: '+response.execResult.exceptionError.error);
  const v=rt.iface.decodeFunctionResult('normalize',hexlify(response.execResult.returnValue));
  rows.push({income:String(v[0]),debt:String(v[1])});
 }
 return {rows,profile:profile.id,kernel:profile.kernel};
}
for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
 try{console.log(JSON.stringify(await normalize(JSON.parse(line))));}
 catch(error){console.log(JSON.stringify({error:error.message}));}
}
