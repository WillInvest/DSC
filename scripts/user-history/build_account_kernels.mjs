/** Build thin fixtures around each reviewed implementation's original Aave libraries. */
import fs from 'node:fs/promises';
import path from 'node:path';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url));
const args=process.argv.slice(2);
const root=path.resolve(args[0]??path.join(here,'../../artifacts/aave-account-kernels-20261115'));
const modules=path.resolve(args[1]??here);
const require=createRequire(path.join(modules,'package.json'));
const {keccak256,toUtf8Bytes}=require('ethers');
const wrapper=require('solc/wrapper');
const repo=path.resolve(here,'../..');
const sha=x=>createHash('sha256').update(x).digest('hex');
const read=async p=>JSON.parse(await fs.readFile(p,'utf8'));
const profileDir=path.join(repo,'contracts/ethereum/aave-profiles');
 const profiles=(await fs.readdir(profileDir)).filter(x=>/^aave-v3-\d+\.json$/.test(x)).sort().map(x=>path.join(profileDir,x));
 for(const x of (await fs.readdir(root)).filter(x=>/^\d+\.profile\.json$/.test(x)).sort())profiles.push(path.join(root,x));
const results=[];
for(const file of profiles){
 const p=await read(file);
 const supplemental=file.startsWith(root+path.sep);
 const sourceRecordPath=supplemental?path.join(root,p.from_block+'.sources.json'):path.join(repo,'scripts/protocols/lending/aave/v3/history/sources',p.implementation.toLowerCase()+'.json');
 const recordBytes=await fs.readFile(sourceRecordPath),record=JSON.parse(recordBytes);
 if(sha(recordBytes)!==p.sources[0].record_sha256)throw Error('Reviewed source record identity differs');
 const runtime=record.runtimeBytecode.onchainBytecode;
 if(sha(Buffer.from(runtime.replace(/^0x/,''),'hex'))!==p.implementation_code_sha256)throw Error('Reviewed implementation runtime identity differs');
 const content=await read(path.join(root,p.from_block+'.sources.json'));
 const sources={};
 for(const [name,entry] of Object.entries(record.metadata.sources)){
  const text=content.sources[name]?.content;
  if(text===undefined||keccak256(toUtf8Bytes(text))!==entry.keccak256)throw Error('Source content mismatch '+p.from_block+' '+name);
  sources[name]={content:text};
 }
 const library=suffix=>{const names=Object.keys(sources).filter(x=>x.endsWith('/'+suffix+'.sol'));if(names.length!==1)throw Error('Ambiguous '+suffix);return names[0];};
 const generic=library('GenericLogic'),prefix=generic.slice(0,-'logic/GenericLogic.sol'.length);
 const types=prefix+'types/DataTypes.sol',reserve=prefix+'logic/ReserveLogic.sol';
 const math=name=>prefix+'math/'+name+'.sol';
 const typesText=sources[types].content;
 const reserveStruct=typesText.match(/struct ReserveData\s*\{([\s\S]*?)\n\s*\}/)[1];
 const categoryStruct=typesText.match(/struct EModeCategory\s*\{([\s\S]*?)\n\s*\}/)[1];
 const paramsStruct=typesText.match(/struct CalculateUserAccountDataParams\s*\{([\s\S]*?)\n\s*\}/)[1];
 const stable=/\baddress stableDebtTokenAddress;/.test(reserveStruct);
 const categoryFields=[...categoryStruct.replace(/\/\/[^\n]*/g,'').matchAll(/\b(?:uint\d+|address|bool|string)\s+(\w+)\s*;/g)].map(x=>x[1]);
 const known=['ltv','liquidationThreshold','liquidationBonus','priceSource','label','collateralBitmap','borrowableBitmap','ltvzeroBitmap','isolated'];
 if(categoryFields.some(f=>!known.includes(f)))throw Error('Unsupported category fields '+categoryFields);
 const fieldValue={ltv:'x.ltv',liquidationThreshold:'x.threshold',liquidationBonus:'x.bonus',priceSource:'x.priceSource',label:"''",collateralBitmap:'x.collateralBitmap',borrowableBitmap:'x.borrowableBitmap',ltvzeroBitmap:'x.ltvzeroBitmap',isolated:'x.isolated'};
 const fixture=`// SPDX-License-Identifier: MIT
pragma solidity >=0.8.0;
import {GenericLogic} from '${generic}';
import {ReserveLogic} from '${reserve}';
import {DataTypes} from '${types}';
import {WadRayMath} from '${math('WadRayMath')}';
${stable?`import {MathUtils} from '${math('MathUtils')}';`:''}
contract BalanceFixture {
 uint256 private scaledAmount;
 uint256 private nominalAmount;
 function set(uint256 scaled,uint256 nominal) external {scaledAmount=scaled;nominalAmount=nominal;}
 function scaledBalanceOf(address) external view returns(uint256){return scaledAmount;}
 function balanceOf(address) external view returns(uint256){return nominalAmount;}
}
contract AccountKernel {
 using ReserveLogic for DataTypes.ReserveData;
 struct ReserveInput {address asset;uint16 id;uint256 configuration;uint128 liquidityIndex;uint128 liquidityRate;uint128 variableBorrowIndex;uint128 variableBorrowRate;uint40 updated;uint256 scaledSupply;uint256 scaledDebt;uint256 stablePrincipal;uint256 stableRate;uint40 stableUpdated;bool hasDebtBalanceOverride;uint256 debtBalanceOverride;}
 struct CategoryInput {uint8 id;uint16 ltv;uint16 threshold;uint16 bonus;address priceSource;uint128 collateralBitmap;uint128 borrowableBitmap;uint128 ltvzeroBitmap;bool isolated;}
 struct Request {ReserveInput[] rows;uint256 userConfiguration;uint8 emode;CategoryInput[] modes;address[] priceAssets;uint256[] assetPrices;}
 mapping(address=>DataTypes.ReserveData) private reserves;
 mapping(uint256=>address) private reserveList;
 mapping(uint8=>DataTypes.EModeCategory) private categories;
 mapping(address=>uint256) private prices;
 uint256[] private previousIds;
 uint8[] private previousCategories;
 address[] private previousPrices;
 function getAssetPrice(address a) external view returns(uint256){return prices[a];}
 function evaluate(Request calldata req) external returns(uint256[6] memory out){
  for(uint256 i=0;i<previousIds.length;i++)delete reserveList[previousIds[i]];
  delete previousIds;
  for(uint256 i=0;i<previousCategories.length;i++)delete categories[previousCategories[i]];
  delete previousCategories;
  for(uint256 i=0;i<previousPrices.length;i++)delete prices[previousPrices[i]];
  delete previousPrices;
  uint256 reserveCount=0;
  for(uint256 i=0;i<req.rows.length;i++){
   ReserveInput calldata x=req.rows[i];
   require(x.updated<=block.timestamp,'future reserve timestamp');
   DataTypes.ReserveData storage r=reserves[x.asset];
   if(r.aTokenAddress==address(0)){r.aTokenAddress=address(new BalanceFixture());r.variableDebtTokenAddress=address(new BalanceFixture());${stable?'r.stableDebtTokenAddress=address(new BalanceFixture());':''}}
   r.configuration.data=x.configuration;r.id=x.id;r.liquidityIndex=x.liquidityIndex;r.currentLiquidityRate=x.liquidityRate;r.variableBorrowIndex=x.variableBorrowIndex;r.currentVariableBorrowRate=x.variableBorrowRate;r.lastUpdateTimestamp=x.updated;
   reserveList[x.id]=x.asset;previousIds.push(x.id);if(x.id+1>reserveCount)reserveCount=x.id+1;
   BalanceFixture(r.aTokenAddress).set(x.scaledSupply,WadRayMath.rayMul(x.scaledSupply,r.getNormalizedIncome()));
   BalanceFixture(r.variableDebtTokenAddress).set(x.scaledDebt,x.hasDebtBalanceOverride?x.debtBalanceOverride:WadRayMath.rayMul(x.scaledDebt,r.getNormalizedDebt()));
   ${stable?`require(x.stablePrincipal==0||x.stableUpdated<=block.timestamp,'future stable timestamp');uint256 balance=x.stablePrincipal==0?0:WadRayMath.rayMul(x.stablePrincipal,MathUtils.calculateCompoundedInterest(x.stableRate,x.stableUpdated,block.timestamp));BalanceFixture(r.stableDebtTokenAddress).set(balance,balance);`:`require(x.stablePrincipal==0,'stable debt removed in this profile');`}
  }
  for(uint256 i=0;i<req.modes.length;i++){CategoryInput calldata x=req.modes[i];DataTypes.EModeCategory storage c=categories[x.id];${categoryFields.map(f=>'c.'+f+'='+fieldValue[f]+';').join('')}previousCategories.push(x.id);}
  require(req.priceAssets.length==req.assetPrices.length,'price vector length');
  for(uint256 i=0;i<req.priceAssets.length;i++){prices[req.priceAssets[i]]=req.assetPrices[i];previousPrices.push(req.priceAssets[i]);}
  (out[0],out[1],out[4],out[3],out[5],)=GenericLogic.calculateUserAccountData(reserves,reserveList,categories,DataTypes.CalculateUserAccountDataParams({userConfig:DataTypes.UserConfigurationMap(req.userConfiguration),${/\breservesCount;/.test(paramsStruct)?'reservesCount:reserveCount,':''}user:address(0xBEEF),oracle:address(this),userEModeCategory:req.emode}));
  out[2]=GenericLogic.calculateAvailableBorrows(out[0],out[1],out[4]);
 }
}`;
 const compilerVersion=record.metadata.compiler.version;
 const compilerPath=path.join(root,'compilers','soljson-v'+compilerVersion+'.cjs');
 const compiler=wrapper(require(compilerPath));
 if(compiler.version().split('.Emscripten')[0]!==compilerVersion)throw Error('Compiler mismatch '+compiler.version());
 const input={language:'Solidity',sources:{...sources,'AccountKernel.sol':{content:fixture}},settings:{remappings:record.metadata.settings.remappings??[],optimizer:{enabled:true,runs:200},evmVersion:'london',outputSelection:{'AccountKernel.sol':{'AccountKernel':['abi','evm.deployedBytecode.object']}}}};
 const compiled=JSON.parse(compiler.compile(JSON.stringify(input)));const errors=(compiled.errors??[]).filter(x=>x.severity==='error');
 if(errors.length)throw Error(errors.map(x=>x.formattedMessage).join('\n'));
 const contract=compiled.contracts['AccountKernel.sol'].AccountKernel;const code='0x'+contract.evm.deployedBytecode.object;
 if(code.includes('_'))throw Error('Unlinked dependency');
 if(p.from_block===25199939){const next=await read(path.join(root,'26392162.profile.json'));if(next.previous_implementation.toLowerCase()!==p.implementation.toLowerCase())throw Error('Supplemental chain does not continue reviewed implementation');p.to_block=next.from_block;}
 const output={profile:String(p.from_block),from_block:p.from_block,to_block_exclusive:p.to_block,implementation:p.implementation,implementation_code_sha256:p.implementation_code_sha256,profile_sha256:sha(await fs.readFile(file)),source_record_sha256:sha(recordBytes),source_record_path:sourceRecordPath,compiler:compilerVersion,compiler_sha256:sha(await fs.readFile(compilerPath)),source_content_keccaks:Object.fromEntries(Object.entries(record.metadata.sources).map(([k,v])=>[k,v.keccak256])),category_fields:categoryFields,stable_debt_supported:stable,fixture_source_sha256:sha(fixture),fixture_source:fixture,abi:contract.abi,code,code_sha256:sha(code),scope:'Original implementation-specific GenericLogic/ReserveLogic and rounding; custom code only maps reconstructed inputs into storage fixtures. Source/protocol identity is recorded; independent node samples remain required.'};
 await fs.writeFile(path.join(root,p.from_block+'.kernel.json'),JSON.stringify(output,null,2));
 results.push({profile:output.profile,from_block:p.from_block,to_block_exclusive:p.to_block,implementation:p.implementation,category_fields:categoryFields,stable_debt_supported:stable,kernel:p.from_block+'.kernel.json',kernel_sha256:sha(await fs.readFile(path.join(root,p.from_block+'.kernel.json')))});
 console.error('Built original Aave profile '+p.from_block);
}
let supplementalEvidence;try{const proofPath=path.join(root,'upgrade-extension-evidence.json');supplementalEvidence={path:'26392162.profile.json',sha256:sha(await fs.readFile(path.join(root,'26392162.profile.json'))),parent_canonical_extension_check:{path:'upgrade-extension-evidence.json',sha256:sha(await fs.readFile(proofPath))}};}catch(e){if(e.code!=='ENOENT')throw e;}
await fs.writeFile(path.join(root,'manifest.json'),JSON.stringify({created_at:new Date().toISOString(),builder_sha256:sha(await fs.readFile(fileURLToPath(import.meta.url))),worker_sha256:sha(await fs.readFile(path.join(here,'account_worker.mjs'))),supplemental_boundary_evidence:supplementalEvidence,profiles:results,field_order:['totalCollateralBase','totalDebtBase','availableBorrowsBase','currentLiquidationThreshold','ltv','healthFactor'],limits:['Reviewed profiles stop at their explicit bounds. Supplemental own-node upgrade evidence extends final reviewed implementation to26392162; new verified profile ends26407882 exclusive. No implicit extension beyond explicit bounds.','Kernel arithmetic is original Aave source. Input reconstruction and version-boundary correctness require independent node validation.']},null,2));
