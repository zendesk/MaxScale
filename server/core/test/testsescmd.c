#include <sescmd.h>
#include <stdio.h>
#include <dcb.h>
#include <buffer.h>

static int cmd_write;
static int cmd_read;

int fakewrite(DCB* dcb, GWBUF* buf)
{
    cmd_write++;
    return 1;
}

int main(int argc, char** argv)
{
    const char* query = "set @test1=1";    
    int rval = 0;
    SCMDLIST* list;
    SCMDCURSOR* cursor;
    DCB* dcb;
    GWBUF* buffer;
    
    cmd_write = 0;
    cmd_read = 0;
    
    dcb = dcb_alloc(DCB_ROLE_REQUEST_HANDLER);
    if(dcb == NULL)
        return 1;
    dcb->state = DCB_STATE_POLLING;
    dcb->func.write = fakewrite;
        
    buffer = gwbuf_alloc(strlen(query)+5);
    gw_mysql_set_byte3((unsigned char*)buffer->start,strlen(query) + 1);    
    gwbuf_set_type(buffer,GWBUF_TYPE_MYSQL);
    *((unsigned char*)buffer->start + 4) = 0x03;
    memcpy(buffer->start + 4,query,strlen(query));   
    
    printf("Allocating session command list... ");    
    if((list = sescmd_allocate()) == NULL)
    {
        printf("Failed to allocate session command list");
        rval = 1;
        goto retblock;
    }
    printf("OK\n");    
    printf("Adding session commands to the list... ");
    if(!sescmd_add_command(list,buffer))
    {
        printf("Failed to add a command to the list\n");
        rval = 1;
        goto retblock;
    }
    if(!sescmd_add_command(list,buffer))
    {
        printf("Failed to add a command to the list\n");
        rval = 1;
        goto retblock;
    }
    printf("OK\n");
    
    printf("Adding a DCB to the list... ");
    if(!sescmd_add_dcb(list,dcb))
    {
        printf("Failed to add a dcb to the list\n");
        rval = 1;
        goto retblock;
    }
    printf("OK\n");
    printf("Waiting for write to fake DCB... ");
    if(cmd_write)
    {
        printf("OK\n");    
    }
    else
    {
        printf("Write to fake DCB failed\n");
        rval = 1;
        goto retblock;
    }

    gw_mysql_set_byte3((unsigned char*)buffer->start,7);
    *((unsigned char*)buffer->start + 3) = 0x01;
    memset(buffer->start + 4,0,7);
    cursor = get_cursor(list,dcb);

    printf("Writing a fake replies to the session commands... ");
    
    sescmd_process_replies(list,dcb,buffer);
    
    if(cmd_write != 2)
    {
        printf("Failed to process first reply\n");
        rval = 1;
        goto retblock;
    }

    sescmd_process_replies(list,dcb,buffer);

    if(cursor->scmd_cur_active)
    {
        printf("Failed to process last reply\n");
        rval = 1;
        goto retblock;
 
    }
    
    printf("OK\n");


    printf("Removing DCB... ");
    
    if(!sescmd_remove_dcb(list,dcb))
    {
        printf("Removing the DCB failed\n");
        rval = 1;
        goto retblock;
    }
    printf("OK\n<");
    
    
    retblock:
    
    gwbuf_free(buffer);    
    dcb_close(dcb);
    return rval;
}
