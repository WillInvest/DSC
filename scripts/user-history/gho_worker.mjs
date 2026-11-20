/** Original historical GHO discount accounting. JSONL, no RPC or file writes. */
import fs from 'node:fs/promises';import path from 'node:path';import readline from 'node:readline';import{createRequire}from'node:module';import{pathToFileURL}from'node:url';
const options=new Map();for(let i=2;i<process.argv.length;i+=2)options.set(process.argv[i],process.argv[i+1]);
const require=options.has('--runtime-root')?createRequire(path.resolve(options.get('--runtime-root'),'package.json')):createRequire(import.meta.url);const load=async n=>import(pathToFileURL(require.resolve(n)).href);
const[{createEVM},{createAddressFromString,Account},{Interface,getBytes,hexlify}]=await Promise.all(['@ethereumjs/evm','@ethereumjs/util','ethers'].map(load));
const artifact=JSON.parse(await fs.readFile(options.get('--artifact')??new URL('./gho-kernel.json',import.meta.url),'utf8')),math=JSON.parse(await fs.readFile(options.get('--math-kernel')??new URL('./math-kernel.json',import.meta.url),'utf8'));
const caller=createAddressFromString('0x0000000000000000000000000000000000012345'),pool=createAddressFromString('0x0000000000000000000000000000000000005555'),mathAddress=createAddressFromString('0x0000000000000000000000000000000000012346'),runtimes=new Map();
const standard=new Set(['0x9b2b73f9ddd830f82d61520388ccf4fc048f9953','0x1b147fc485c1952b7e76fa99e9af0d5cb2ba9a79','0xc4bea6ff17879e27f266909397e5e0ad3d301946']);
async function call(evm,address,iface,name,args){const r=await evm.runCall({to:address,caller,data:getBytes(iface.encodeFunctionData(name,args)),gasLimit:10000000n});if(r.execResult.exceptionError)throw Error('Original GHO/Aave reverted: '+r.execResult.exceptionError.error);return iface.decodeFunctionResult(name,hexlify(r.execResult.returnValue));}
async function runtime(implementation){
 if(!runtimes.has(implementation)){
  const k=artifact.implementations[implementation];if(!k&&!standard.has(implementation))throw Error('Unsupported GHO implementation '+implementation);
  const evm=await createEVM();await evm.stateManager.putAccount(caller,new Account());await evm.stateManager.putAccount(mathAddress,new Account());await evm.stateManager.putCode(mathAddress,getBytes(math.code));
  const rt={evm,math:new Interface(math.abi),kernel:k,address:null,iface:null};
  if(k){await evm.stateManager.putAccount(pool,new Account());await evm.stateManager.putCode(pool,getBytes(k.poolCode));const r=await evm.runCall({caller,data:getBytes(k.creation),gasLimit:100000000n});if(r.execResult.exceptionError||!r.createdAddress)throw Error('GHO fixture construction failed: '+(r.execResult.exceptionError?.error??'missing address')+' '+hexlify(r.execResult.returnValue));rt.address=r.createdAddress;rt.iface=new Interface(k.abi);}
  runtimes.set(implementation,rt);
 }
 return runtimes.get(implementation);
}
function state(input={}){return{shares:String(input.shares??'0'),previous_index:String(input.previous_index??'0'),discount_percent:String(input.discount_percent??'0')};}
async function inspect(rt,s,index){return call(rt.evm,rt.address,rt.iface,'inspect',[BigInt(s.shares),BigInt(s.previous_index),BigInt(s.discount_percent),BigInt(index)]);}
async function handle(p){
 if(p.operation==='balance'){
  if(p.previous_index===undefined||p.discount_percent===undefined)throw Error('Missing historical GHO discount state');
  const impl=p.implementation.toLowerCase(),rt=await runtime(impl);if(!rt.kernel)throw Error('Use standard TokenMath for non-discount GHO implementation');
  return{value:String((await inspect(rt,state(p),p.index))[0])};
 }
 if(p.operation!=='replay')throw Error('Unsupported GHO operation');
 const values=new Map(Object.entries(p.initial??{}).map(([a,s])=>[a.toLowerCase(),state(s)])),pending=new Map(),history=[],gho_history=[];
 for(const row of p.events){
  if(row.name==='DiscountPercentUpdated'){
   const owner=row.arg__user.toLowerCase(),s=values.get(owner)??state();if(String(row.arg__oldDiscountPercent)!==s.discount_percent)throw Error('GHO old discount state differs for '+owner);if(pending.has(owner))throw Error('Unconsumed GHO discount transition');pending.set(owner,{old:s.discount_percent,next:String(row.arg__newDiscountPercent)});continue;
  }
  if(!['Mint','Burn'].includes(row.name))throw Error('Unsupported GHO balance event '+row.name);
  const owner=(row.name==='Mint'?row.arg__onBehalfOf:row.arg__from).toLowerCase(),s=values.get(owner)??state(),before=BigInt(s.shares),impl=p.profiles[String(row.schema_version)]?.toLowerCase();if(!impl)throw Error('Missing GHO historical implementation');const rt=await runtime(impl),index=BigInt(row.arg__index),discount=pending.get(owner);let next;
  if(rt.kernel){
   const input={...s,discount_percent:discount?.old??s.discount_percent};const original=await inspect(rt,input,index);
   if(String(original[1])!==String(row.arg__balanceIncrease))throw Error('GHO original accrued interest differs from emitted balanceIncrease');
   const signed=row.name==='Mint'?BigInt(row.arg__value)-BigInt(row.arg__balanceIncrease):-BigInt(row.arg__value)-BigInt(row.arg__balanceIncrease);
   const kind=signed>0n?0:signed<0n?1:2,amount=signed<0n?-signed:signed;
   const actual=await call(rt.evm,rt.address,rt.iface,'transition',[before,BigInt(s.previous_index),BigInt(input.discount_percent),index,amount,kind,BigInt(discount?.next??s.discount_percent)]);
   next=actual[0];
   if(actual[1]!==index||String(actual[2])!==(discount?.next??s.discount_percent))throw Error('Original GHO transition state differs from emitted index/discount');
  }else{
   if(discount)throw Error('Discount update in standard GHO implementation');
   next=(await call(rt.evm,mathAddress,rt.math,'eventBalance',[before,BigInt(row.arg__value),BigInt(row.arg__balanceIncrease),index,row.name==='Mint',p.modern_versions.includes(row.schema_version),true]))[0];
  }
  if(next<0n)throw Error('Negative reconstructed GHO shares');
  const after={shares:String(next),previous_index:String(index),discount_percent:rt.kernel?(discount?.next??s.discount_percent):'0'};values.set(owner,after);pending.delete(owner);
  const keys={owner,block:row.block_number,transaction_index:row.transaction_index,log_index:row.log_index};gho_history.push({...keys,previous_index:after.previous_index,discount_percent:after.discount_percent});
  if(next!==before)history.push({...keys,before:String(before),after:String(next),delta:String(next-before),event:row.name,index:String(index)});
 }
 if(pending.size)throw Error('GHO discount transition missing associated Mint/Burn in complete input chunk');
 return{balances:Object.fromEntries([...values].map(([a,s])=>[a,s.shares])),states:Object.fromEntries(values),history,gho_history};
}
for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
 try{console.log(JSON.stringify(await handle(JSON.parse(line))));}catch(e){console.log(JSON.stringify({error:e.message}));}
}
