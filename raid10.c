// usage example:
// sudo ./raid10 /dev/sdb /dev/sdc

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h> // for open flags
#include <assert.h>
#include <errno.h> 
#include <string.h>

#define DEVICE_SIZE (1024*1024*256) // assume all devices identical in size

#define SECTOR_SIZE 512
#define SECTORS_PER_BLOCK 4
#define BLOCK_SIZE (SECTOR_SIZE * SECTORS_PER_BLOCK)

#define BUFFER_SIZE (BLOCK_SIZE * 2)

char	buf[BUFFER_SIZE];
int		num_dev;
int     raid1_index;  //the index of the raid1 we need
int     disk_index;  //the index of the disk we need
int     num_of_raid1;  //# of raid1's
int     disks_in_raid1; //# disks in each raid1
int		*dev_fd;

void do_raid10_rw(char* operation, int sector, int count)
{
	int j;  //index for loops
	int i = sector;
	int lseekVal, readVal, writeVal;

	while (i < sector+count)
	{
		
		int block_num = i / SECTORS_PER_BLOCK;  
		
		raid1_index = block_num % num_of_raid1;
		disk_index = raid1_index * disks_in_raid1;
		
		int block_start = (disks_in_raid1 * sector) / (disks_in_raid1 * num_of_raid1 * SECTORS_PER_BLOCK);

		
		int block_off = i % SECTORS_PER_BLOCK;  
		int sector_start = block_start * SECTORS_PER_BLOCK + block_off; 
		int offset = sector_start * SECTOR_SIZE;  
		
		// try to write few sectors at once
		int num_sectors = SECTORS_PER_BLOCK - block_off;  
		while (i+num_sectors > sector+count)
			--num_sectors;
		int sector_end = sector_start + num_sectors - 1;  
		int size = num_sectors * SECTOR_SIZE;  
		
		// validate calculations
		if(num_sectors <= 0){
		 printf("Error in calculations in do_raid10_rw function :  num_sectors should be > 0.  Operation aborted\n");
		 return;
		}
		if( size > BUFFER_SIZE){
		  printf("Error in calculations in do_raid10_rw function : BUFFER_SIZE < size .  Operation aborted\n");
		 return;
		}
		if( (offset+size)  > DEVICE_SIZE){
		 printf("Error in calculations in do_raid10_rw function :  offset+size > DEVICE_SIZE . Operation aborted\n");
		 return;
		}
		
		
		int failedDisks = 0;
		if (!strcmp(operation, "READ")) {
		  for(j=0; j <= disks_in_raid1; j++){
		    
		    if(dev_fd[disk_index + j] >= 0){  //if device isn't bad
		      lseekVal = lseek(dev_fd[disk_index + j], offset, SEEK_SET);
		      if(lseekVal == offset){  

			readVal = read(dev_fd[disk_index + j], buf, size);
			if(readVal == size){
			  printf("Operation on device %d, sector %d-%d\n",
			  disk_index + j, sector_start, sector_end);
			  j = disks_in_raid1 + 2;  //exit for loop on success
			  break;
			}
			else{  //error in read syscall. But operation continues
			  printf("Error in read syscall on device %d : %s\n   		However operation continues due to available backup devices\n", disk_index + j,  strerror( errno ));
			  
			  //kill device:
			  close(dev_fd[disk_index + j]);
			  dev_fd[disk_index + j] = -1;
			}
		      
		      }
		      else{  //error in lseek syscall.  But operation continues
			printf("Error in lseek on device %d : %s\n . However  operation continues due to available backup devices.\n", disk_index + j, strerror( errno ));
			
			//kill device:
			close(dev_fd[disk_index + j]);
			dev_fd[disk_index + j] = -1;
		      }
		    
		    } //closes case where dev_fd[disk_index]  >=0
		    
		    ++failedDisks;
		    
		    if( failedDisks == disks_in_raid1  ){ //error
		      printf("Operation on bad device %d . READ operation aborted\n", disk_index + j);
		      return;
		    }
		    
		  } //closes for loop	
		} //closes case of READ command
		
	
		else if (!strcmp(operation, "WRITE")) {
			//move offset in all disks in the correct raid1 device and write simultanesouly
			for(j=0; j < disks_in_raid1; j++){
			  
			  if(dev_fd[disk_index + j] >= 0){  //if device isn't bad
			    lseekVal = lseek(dev_fd[disk_index + j], offset, SEEK_SET);
			    if(lseekVal == offset){
			      writeVal = write(dev_fd[disk_index + j], buf, size);
			      if(writeVal == size){
				printf("Operation on device %d, sector %d-%d\n",
				  disk_index + j, sector_start, sector_end);
			      }
			      else{
				printf("Error in write syscall on device %d : %s\nHowever operation continues due to available backup devices\n", disk_index + j,  strerror( errno ));
				++failedDisks;
				
				//kill device:
				close(dev_fd[disk_index + j]);
				dev_fd[disk_index + j] = -1;
			      }
			    }
			    else{ //error in lseek syscall
			      printf("Error in lseek on device %d : %s\n . However  operation continues due to available backup devices.\n", disk_index + j, strerror( errno ));
			      ++failedDisks;
			      
			      //kill device:
			      close(dev_fd[disk_index + j]);
			      dev_fd[disk_index + j] = -1;
			    }
			   
			  }
			  
			  else{ //bad device
			     ++failedDisks;
			     //kill device:
			     close(dev_fd[disk_index + j]);
			     dev_fd[disk_index + j] = -1;
			  }
			  
			  if(failedDisks == disks_in_raid1){ //error
			    printf("Operation on bad device %d . WRITE operation aborted\n", disk_index + j);
			    return;
			  }
			  
			} //closes for loop
		} //closes WRITE operation case
		
		
		
		i += num_sectors;
	}//closes while
}


void do_raid10_repair(int index, char *device_name)
{
  
  int newDevice_fd = open(device_name, O_RDWR);
  if (newDevice_fd < 0){
    printf( "Error opening new device file: %s\n", strerror( errno ) );
    return;
  }
  
  int offset, bytes_operated;
  int readReturn, lseekReturn, writeReturn;
  //calculate the smallest disk index in the inner RAID1 we are in:
  int calc_modulo = index % disks_in_raid1;
  int smallest_index = index - calc_modulo;
  int oldIndex = index;
  index = smallest_index;
  int goodDevices = 0;  //number of devices which aren't faulty
  int replace = 1;      //replace the 2 devices iff replace == 1
  
  int j;   
  for(offset = 0;  offset < DEVICE_SIZE; offset += BUFFER_SIZE){  //we were told we can use this buffer
    
      for(j=0; j < disks_in_raid1; j++){
	  goodDevices = 0;
	  if( (index +j)  == oldIndex )
	      continue;  //treat the old device as faulty
	  if(dev_fd[index +j] >= 0){  
	      ++goodDevices;
	      lseekReturn = lseek(dev_fd[index + j], offset, SEEK_SET);
	      if(lseekReturn == offset){  
		  readReturn = read(dev_fd[index + j], buf, BUFFER_SIZE);
		  if(readReturn == BUFFER_SIZE){
		      writeReturn = write(dev_fd[index + j], buf, BUFFER_SIZE);
		      if(writeReturn == BUFFER_SIZE){
			  j = disks_in_raid1 + 2;  //exit inner for loop on success
			  //break;
		      }
		      else{  //error in write syscall
			  offset = DEVICE_SIZE + 10;     //stop the run
			  j = disks_in_raid1 + 2;
			  printf("Error in write syscall on new device : %s\nREPAIR command aborted\n",  strerror( errno ));
			  //kill the new device:
			  close(newDevice_fd);
			  newDevice_fd = -1;
			  replace = 0;   //do not replace the 2 devices
			  break;
		      }
		      
		  } //closes if(readReturn == BUFFER_SIZE) 
		  else{  //error in read syscall from old devices.
		    printf("Error in read syscall on device %d : %s\n", index + j, strerror( errno ));
		    close(dev_fd[index + j]);
		    dev_fd[index + j] = -1;    //kill old device
		  }
	      } //closes lseekReturn == offset
	      else{  //error in lseek
		printf("Error in lseek on device %d : %s\n", index + j, strerror( errno ));
		close(dev_fd[index + j]);
		dev_fd[index + j] = -1;    //kill old device
	      }
			  
			
	  }//closes if(dev_fd[index] >= 0)
      
      }//closes inner for
      
      if(goodDevices == 0){  //all devices are faulty.  stop the restoration
	  offset = DEVICE_SIZE + 10;    
	  j = disks_in_raid1 + 2;
	  printf("Operation on bad device:  All old devices are faulty\n");
	  break;
      }
  }//closes outer for
  
  //on success replace the devices:
  if(replace == 1){
      if(dev_fd[oldIndex] >= 0)
	  close(dev_fd[oldIndex]);
      dev_fd[oldIndex] = newDevice_fd;
  }
  
}//closes REPAIR func

int main(int argc, char** argv)
{
	
	if(argc < 6){
	 printf("Error:  there should be at least 4 devices given in command line\n");
	 return 1;
	}
	
	int i;
	int scanf_val;
	char line[1024];
	
	// number of devices == number of arguments (ignore 1st,2nd)
	
	num_dev = argc-2;
	int _dev_fd[num_dev];
	dev_fd = _dev_fd;
	
	disks_in_raid1 = atoi(argv[1]);
	num_of_raid1 = num_dev / disks_in_raid1;  //we assume num_dev is a multiple of disks_in_raid1
	
	// open all devices       
	for (i = 0; i < num_dev; ++i) {
		printf("Opening device %d: %s\n", i, argv[i+2]);
		dev_fd[i] = open(argv[i+2], O_RDWR);
		if (dev_fd[i] < 0) {
		  printf( "Error opening device file: %s\nDevice will be KILLED. However, operation continues\n", strerror( errno ) );
		}
	}
	
	// vars for parsing input line
	char operation[20];
	char int_or_string[100];
	int sector;
	int count;
	
	// read input lines to get command of type "OP <SECTOR> <COUNT>"
	while (fgets(line, 1024, stdin) != NULL) {
		
	  scanf_val = sscanf(line, "%s %d %s", operation, &sector, int_or_string);
	  if(scanf_val != 3){
	   printf("Error in scanf func:  %s\n", strerror( errno ) );
	   return errno;  
	  }
	  
	
		// KILL specified device
		if (!strcmp(operation, "KILL")) {
			close(dev_fd[sector]);
			dev_fd[sector] = -1;
		}
		
		else if (!strcmp(operation, "REPAIR")) {
		  do_raid10_repair(sector, int_or_string);
		   
		}
		// READ / WRITE
		else {
		  count = atoi(int_or_string);
		  do_raid10_rw(operation, sector, count);
		}		
	}
	
	for(i=0; i < num_dev; i++) {  //we were told there's no need to check return value of close syscall
	  if (dev_fd[i] >= 0)
	    close(dev_fd[i]);	 	
	}
}
