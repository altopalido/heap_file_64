#include "heap_file.h"
#include "bf.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>


HP_ErrorCode HP_Init() {
  return HP_OK;
}





HP_ErrorCode HP_CreateIndex(const char *filename) {
  BF_Block *BlockMetaData; // Block gia ton anagnwristiko arithmo 210
  BF_Block *dataBlock; // Arxiko Block, posa records einai grammena sto arxeio
  int fileDesc, error;
  char* BlockMetaDataCont; // metavlhth gia to periexomeno tou BlockMetaData
  char* dataBlockCont; // metavlhth gia to periexomeno tou dataBlock
  error = BF_CreateFile(filename) ; // Dimiourgoume to arxeio
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR; 
  }
  error = BF_OpenFile(filename,&fileDesc); // Anoigoume to arxeio pou dimiourghsame
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  BF_Block_Init(&BlockMetaData); // Arxikopoioume
  error = BF_AllocateBlock(fileDesc,BlockMetaData);
  if(error) 
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  BlockMetaDataCont = BF_Block_GetData(BlockMetaData); // Getting block Content
  *(int*)BlockMetaDataCont = 210; // arithmos 210 gia anagnwristiko heap file
  BF_Block_SetDirty(BlockMetaData); // kanoume dirty to block wste meta na graftei sto disko
  error = BF_UnpinBlock(BlockMetaData); // kanoume unpin wste na eleutherwsoume ton buffer
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  BF_Block_Init(&dataBlock); // Arxikopoioume to dataBlock
  error = BF_AllocateBlock(fileDesc,dataBlock);
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  dataBlockCont = BF_Block_GetData(dataBlock);
  *(int*)dataBlockCont = 0; // Arxikopoiw ton arithmo twn egrafwn se 0
  BF_Block_SetDirty(dataBlock);
  error = BF_UnpinBlock(dataBlock);
  if(error)
  {
    BF_PrintError(error); 
    return HP_ERROR;
  }
  error = BF_CloseFile(fileDesc); //kleinoume to arxeio
  if(error)
  { 
    BF_PrintError(error);
    return HP_ERROR;
  }
  return HP_OK;
}





HP_ErrorCode HP_OpenFile(const char *fileName, int *fileDesc){
  BF_Block *BlockMetaData;
  BF_Block_Init(&BlockMetaData); // Arxikopoihsh tou block
  char* BlockMetaDataCont;
  int error;
  error = BF_OpenFile(fileName,fileDesc); // anoigoume to arxeio me onoma filename
  if(error)
  {
    BF_PrintError(error); 
    return HP_ERROR;
  }
  error = BF_GetBlock(*fileDesc, 0, BlockMetaData); 
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  BlockMetaDataCont = BF_Block_GetData(BlockMetaData); //kratame to periexomeno to BlockMetaData sth metavlhth BlockMetaData Cont
  if(*(int*)BlockMetaDataCont != 210) // An den einai heap file
  {
    BF_PrintError(BF_INVALID_FILE_ERROR);
    return HP_ERROR;
  }
  error = BF_UnpinBlock(BlockMetaData);
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  return HP_OK;
}






HP_ErrorCode HP_CloseFile(int fileDesc) {
  int error;
  error = BF_CloseFile(fileDesc); //dinoume sth metavlhth error thn timh pou epistrefei h sunarthsh
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  return HP_OK;
}







HP_ErrorCode HP_InsertEntry(int fileDesc, Record record) {
  int NumOfBlocks; //metavlhth sthn opoia kataxwrw ton arithmo twn blocks
  BF_Block *currentBlock; // to current block
  char* currentBlockData; // metavlhth gia ta dedomena tou current block
  int* recordCount;
  int error;
	
  BF_Block_Init(&currentBlock);
  error = BF_GetBlockCounter(fileDesc, &NumOfBlocks);
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  error = BF_GetBlock(fileDesc, NumOfBlocks-1, currentBlock); // kataxwrhse to block sto currentBlock 
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  currentBlockData = BF_Block_GetData(currentBlock); //kataxwrhsh twn dedomenwn tou currentBlock
  recordCount = (int*)currentBlockData;
  if(*(recordCount) == ((1024-sizeof(int))/(sizeof(Record)))) // an to block einai gemato tote ginontai ta parakatw
  {
    BF_Block *Block; //ftiaxnw kainourio block
    BF_Block_Init(&Block); // kai to arxikopoiw
    error = BF_UnpinBlock(currentBlock); // kanw unpin to palio block
    if(error)
    {
      BF_PrintError(error);
      return HP_ERROR;
    }
    currentBlock = Block;
    error = BF_AllocateBlock(fileDesc, currentBlock); 
    if(error)
    {
      BF_PrintError(error);
      return HP_ERROR;
    }
    currentBlockData = BF_Block_GetData(currentBlock); 
    *(int*)currentBlockData = 0; // arxika exw 0 records
    recordCount = (int*)currentBlockData;
  }
  memcpy(currentBlockData + sizeof(int) + (*recordCount) * sizeof(Record), &record, sizeof(Record)); // grafoume to neo record
  (*recordCount)++; // auksanoume kata ena th metavlhth 
  BF_Block_SetDirty(currentBlock); 
  error = BF_UnpinBlock(currentBlock);
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  return HP_OK;
}
	 
	 
	 
	 
	 
	 

HP_ErrorCode HP_PrintAllEntries(int fileDesc) {
  int i,j;
  int error;
  int NumOfBlock, recordCount;
  Record currentRecord;
  BF_Block *currentBlock, *endBlock;
  BF_Block_Init(&currentBlock);
  BF_Block_Init(&endBlock);
  char* blockData;

  error = BF_GetBlockCounter(fileDesc,&NumOfBlock);
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  for (i = 1; i < NumOfBlock; i++) //gia ola ta blocks
  {
    error = BF_GetBlock(fileDesc, i, currentBlock); 
    if(error)
    {
      BF_PrintError(error); 
      return HP_ERROR;
    }
    blockData = BF_Block_GetData(currentBlock);
    recordCount = *(int*)blockData;
    for (j = 0; j < recordCount; j++) // gia oles tis eggrafes
    {
      memcpy(&currentRecord, blockData + sizeof(int) + j*sizeof(Record), sizeof(Record)); // Antigrafoume ta dedomena
      printf("\n %d %s %s %s", currentRecord.id, currentRecord.name, currentRecord.surname, currentRecord.city);
    }
    error = BF_UnpinBlock(currentBlock); //unpin to block tou opoiou tupwsame oles tis egrafes
    if(error)
    {
      BF_PrintError(error);
      return HP_ERROR;
    }
  }
  return HP_OK;
}
	 
	 
	 
	 
	 
	 

HP_ErrorCode HP_GetEntry(int fileDesc, int rowId, Record *record) {
  int error;
  int BlockNumberOfRecord; //o arithmos tou block pou vrisketai h egrafh
  int EntryNumberOfRecord; 
  int blockCount;
  BF_Block *Block;
  char* blockData;

  BlockNumberOfRecord = (rowId / ((1024-sizeof(int))/(sizeof(Record))));
  EntryNumberOfRecord = (rowId % ((1024-sizeof(int))/(sizeof(Record))));

  BF_Block_Init(&Block);
  error = BF_GetBlockCounter(fileDesc,&blockCount); 
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }
  if (blockCount < BlockNumberOfRecord) // An o arithmos twn block einai mikroteros apo ton arithmo tou block pou einai h egrafh pou psaxnoume
  {
    return HP_ERROR;
  }
  error = BF_GetBlock(fileDesc, BlockNumberOfRecord, Block); // kataxwroume sth metavlhth block, to block noumero BlockNumberOfRecord
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }

  blockData = BF_Block_GetData(Block);
  memcpy(blockData + sizeof(int) + EntryNumberOfRecord*sizeof(Record), &record,  sizeof(Record));

  error = BF_UnpinBlock(Block);
  if(error)
  {
    BF_PrintError(error);
    return HP_ERROR;
  }

  return HP_OK;
}
