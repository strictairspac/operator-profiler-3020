/** @typedef {{id:number,name:string,sensors:object[],readings:object[]}} Dataset */
/** @typedef {{id:number,name:string,dataset_id:number,steps:object[]}} Plan */
/** @typedef {{id:number,name:string,status:'queued'|'running'|'completed'|'failed',result:object|null}} Trace */
/** @typedef {{id:number,dataset_id:number,fields:string[],fields_key:string,status:'building'|'available'|'failed',version:string|null,entry_count:number,checksum:string|null,error:string|null}} IndexMeta */
export {};
