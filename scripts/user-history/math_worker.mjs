import fs from 'node:fs/promises';
import readline from 'node:readline';
import {createEVM} from '@ethereumjs/evm';
import {createAddressFromString,Account} from '@ethereumjs/util';
import {Interface,getBytes,hexlify} from 'ethers';

const kernel=JSON.parse(await fs.readFile(new URL('./math-kernel.json',import.meta.url),'utf8'));
const evm=await createEVM(),address=createAddressFromString('0x0000000000000000000000000000000000012345'),iface=new Interface(kernel.abi);
await evm.stateManager.putAccount(address,new Account());await evm.stateManager.putCode(address,getBytes(kernel.code));
async function call(name,args){const r=await evm.runCall({to:address,caller:address,data:getBytes(iface.encodeFunctionData(name,args)),gasLimit:1000000n});if(r.execResult.exceptionError)throw Error('Original math: '+r.execResult.exceptionError.error);return iface.decodeFunctionResult(name,hexlify(r.execResult.returnValue))[0];}
for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
  try{
    const p=JSON.parse(line);
    if(p.operation==='balance'){console.log(JSON.stringify({value:String(await call('balance',[BigInt(p.shares),BigInt(p.index),p.modern,p.debt]))}));continue;}
    const values=new Map(Object.entries(p.initial).map(([a,b])=>[a,BigInt(b)])),history=[];
    function change(owner,next,row){const before=values.get(owner)??0n;if(next<0n)throw Error('Negative scaled position');values.set(owner,next);if(before!==next)history.push({owner,before:String(before),after:String(next),delta:String(next-before),block:row.block_number,transaction_index:row.transaction_index,log_index:row.log_index,event:row.name,index:row.arg__index});}
    for(const row of p.events){
      if(row.name==='BalanceTransfer'){
        if(row.arg__from===row.arg__to)continue;
        // BalanceTransfer records actual scaled storage movement, including a
        // zero-address recipient. Mint/Burn, not the address, define issuance.
        const shares=BigInt(row.arg__value);for(const [owner,sign] of [[row.arg__from,-1n],[row.arg__to,1n]]){change(owner,(values.get(owner)??0n)+sign*shares,row);}
      }else if(row.name==='Mint'||row.name==='Burn'){
        const owner=row.name==='Mint'?row.arg__onBehalfOf:row.arg__from;
        if(!owner)throw Error('Unknown balance-owner field');
        const before=values.get(owner)??0n;
        const next=await call('eventBalance',[before,BigInt(row.arg__value),BigInt(row.arg__balanceIncrease),BigInt(row.arg__index),row.name==='Mint',p.modern_versions.includes(row.schema_version),p.debt]);
        change(owner,next,row);
      }
    }
    console.log(JSON.stringify({balances:Object.fromEntries([...values].map(([a,b])=>[a,String(b)])),history}));
  }catch(e){console.log(JSON.stringify({error:e.message}));}
}
