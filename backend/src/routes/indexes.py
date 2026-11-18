from fastapi import APIRouter,Depends
from src.middlewares.auth import current,writer
from src.schemas.business import IndexInput
from src.services import indexes
from src.controllers.business import create_index,retry_index,delete_index
router=APIRouter()
@router.get('/indexes')
def listing(dataset_id:int|None=None,u=Depends(current)):return indexes.list_items(dataset_id)
@router.post('/indexes')
def create(data:IndexInput,u=Depends(writer)):return create_index(data,u)
@router.post('/indexes/{item_id}/retry')
def retry(item_id:int,u=Depends(writer)):return retry_index(item_id,u)
@router.delete('/indexes/{item_id}')
def remove(item_id:int,u=Depends(writer)):return delete_index(item_id,u)
