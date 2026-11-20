/** Version-pinned local execution of original Aave GenericLogic. JSON lines in/out. */
import fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url));
const root=path.resolve(process.argv[2]??path.join(here,'../../artifacts/aave-account-kernels-20261115'));
const modules=path.resolve(process.argv[3]??here),require=createRequire(path.join(modules,'package.json'));
const {createEVM}=require('@ethereumjs/evm');const {createBlock}=require('@ethereumjs/block');
const {createAddressFromString,Account}=require('@ethereumjs/util');const {Interface,getBytes,hexlify}=require('ethers');
const manifest=JSON.parse(await fs.readFile(path.join(root,'manifest.json'),'utf8'));
const sha=x=>createHash('sha256').update(x).digest('hex');const cache=new Map();
const reserveFields=['asset','id','configuration','liquidityIndex','liquidityRate','variableBorrowIndex','variableBorrowRate','updated','scaledSupply','scaledDebt','stablePrincipal','stableRate','stableUpdated'];
const categoryFields=['id','ltv','threshold','bonus','priceSource','collateralBitmap','borrowableBitmap','ltvzeroBitmap','isolated'];
const zero='0x0000000000000000000000000000000000000000';
async function kernel(profile){
 if(cache.has(profile))return cache.get(profile);
 const info=manifest.profiles.find(x=>x.profile===profile);if(!info)throw Error('Unsupported profile '+profile);
 const bytes=await fs.readFile(path.join(root,info.kernel));if(sha(bytes)!==info.kernel_sha256)throw Error('Kernel artifact identity differs');const data=JSON.parse(bytes);
 const evm=await createEVM(),address=createAddressFromString('0x0000000000000000000000000000000000012345');await evm.stateManager.putAccount(address,new Account());await evm.stateManager.putCode(address,getBytes(data.code));
 const k={info,data,evm,address,iface:new Interface(data.abi)};cache.set(profile,k);return k;
}
function request(account,requiredCategoryFields,inputFields){
 const ids=new Set(),assets=new Set();
 for(const r of account.rows){if(ids.has(String(r.id))||assets.has(r.asset.toLowerCase()))throw Error('Duplicate reserve id/asset');ids.add(String(r.id));assets.add(r.asset.toLowerCase());for(const key of reserveFields)if(r[key]===undefined)throw Error('Missing reserve input '+key);}
 const config=BigInt(account.userConfiguration);for(let i=0;i<128;i++)if(((config>>BigInt(i*2))&3n)!==0n&&!ids.has(String(i)))throw Error('User flags reference missing reserve id '+i);
 const modes=account.modes??[];if(Number(account.emode)!==0&&!modes.some(x=>Number(x.id)===Number(account.emode)))throw Error('Missing selected eMode category');
 const aliases={liquidationThreshold:'threshold',liquidationBonus:'bonus'};
 for(const c of modes)for(const field of requiredCategoryFields){if(field==='label')continue;const key=aliases[field]??field;if(c[key]===undefined)throw Error('Missing historical eMode field '+key);}
 const categories=modes.map(c=>categoryFields.map(k=>c[k]??(k==='priceSource'?zero:k==='isolated'?false:'0')));
 const prices=new Map(account.priceAssets.map((a,i)=>[a.toLowerCase(),account.assetPrices[i]]));if(prices.size!==account.priceAssets.length)throw Error('Duplicate price asset');
 for(const r of account.rows){if(!prices.has(r.asset.toLowerCase()))throw Error('Missing reserve oracle price');if(BigInt(prices.get(r.asset.toLowerCase()))<=0n)throw Error('Nonpositive reserve price');}
 for(const c of modes)if(c.priceSource&&c.priceSource.toLowerCase()!==zero&&!prices.has(c.priceSource.toLowerCase()))throw Error('Missing eMode oracle source price');
 if(account.rows.some(r=>r.hasDebtBalanceOverride)&&!inputFields.includes('hasDebtBalanceOverride'))throw Error('Kernel does not support nominal debt override');
 return [account.rows.map(r=>inputFields.map(k=>r[k]??(k==='hasDebtBalanceOverride'?false:k==='debtBalanceOverride'?'0':undefined))),account.userConfiguration,account.emode,categories,account.priceAssets,account.assetPrices];
}
for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
 let p;
 try{
  p=JSON.parse(line);if(p.operation!=='evaluate')throw Error('Unsupported operation');if(!Number.isSafeInteger(p.block)||!Number.isSafeInteger(p.timestamp))throw Error('Integer block/timestamp required');
  const profile=String(p.profile),k=await kernel(profile);if(p.block<k.info.from_block||p.block>=k.info.to_block_exclusive)throw Error('Block outside reviewed profile bounds');
  const results=[];
  for(const a of p.accounts){
   for(const r of a.rows)if(BigInt(r.updated)>BigInt(p.timestamp)||BigInt(r.stablePrincipal)>0n&&BigInt(r.stableUpdated)>BigInt(p.timestamp))throw Error('Future interest timestamp');
   const inputFields=k.data.abi.find(x=>x.name==='evaluate').inputs[0].components.find(x=>x.name==='rows').components.map(x=>x.name);
   const req=request(a,k.data.category_fields,inputFields),block=createBlock({header:{timestamp:BigInt(p.timestamp),number:BigInt(p.block)}});
   // Revert fixture mutation after every account, even on failure. No account can
   // inherit another account's reserves, prices, modes or token fixture balances.
   await k.evm.stateManager.checkpoint();
   let r;
   try{r=await k.evm.runCall({to:k.address,caller:k.address,data:getBytes(k.iface.encodeFunctionData('evaluate',[req])),gasLimit:100000000n,block});}
   finally{await k.evm.stateManager.revert();}
   if(r.execResult.exceptionError)throw Error('Original Aave evaluate: '+r.execResult.exceptionError.error+' '+hexlify(r.execResult.returnValue));
   const account=[...k.iface.decodeFunctionResult('evaluate',hexlify(r.execResult.returnValue))[0]].map(String);results.push({id:a.id,account});
  }
  console.log(JSON.stringify({profile,block:p.block,timestamp:p.timestamp,results}));
 }catch(e){console.log(JSON.stringify({error:e.message,profile:p?.profile,block:p?.block}));}
}
