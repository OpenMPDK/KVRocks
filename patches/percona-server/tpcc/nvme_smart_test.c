#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <linux/nvme_ioctl.h>
#include <fcntl.h>
#include <linux/types.h>
#include <string.h>

typedef unsigned long long uint64_t;


    //////////////////////////////////////////////////////
    // for capacity from device
    struct nvme_lbaf {
            __le16                  ms;
            __u8                    ds;
            __u8                    rp;
    };
    
/*
    struct nvme_admin_cmd {
            __u8    opcode;
            __u8    flags;
            __u16   rsvd1;
            __u32   nsid;
            __u32   cdw2;
            __u32   cdw3;
            __u64   metadata;
            __u64   addr;
            __u32   metadata_len;
            __u32   data_len;
            __u32   cdw10;
            __u32   cdw11;
            __u32   cdw12;
            __u32   cdw13;
            __u32   cdw14;
            __u32   cdw15;
            __u32   timeout_ms;
            __u32   result;
    };
*/
    struct nvme_id_ns {
            __le64                  nsze;
            __le64                  ncap;
            __le64                  nuse;
            __u8                    nsfeat;
            __u8                    nlbaf;
            __u8                    flbas;
            __u8                    mc;
            __u8                    dpc;
            __u8                    dps;
            __u8                    nmic;
            __u8                    rescap;
            __u8                    fpi;
            __u8                    dlfeat;
            __le16                  nawun;
            __le16                  nawupf;
            __le16                  nacwu;
            __le16                  nabsn;
            __le16                  nabo;
            __le16                  nabspf;
            __le16                  noiob;
            __u8                    nvmcap[16];
            __u8                    rsvd64[28];
            __le32                  anagrpid;
            __u8                    rsvd96[3];
            __u8                    nsattr;
            __le16                  nvmsetid;
            __le16                  endgid;
            __u8                    nguid[16];
            __u8                    eui64[8];
            struct nvme_lbaf        lbaf[16];
            __u8                    rsvd192[192];
            __u8                    vs[3712];
    };
    

    struct nvme_id_power_state {
    	__le16			max_power;	/* centiwatts */
    	__u8			rsvd2;
    	__u8			flags;
    	__le32			entry_lat;	/* microseconds */
    	__le32			exit_lat;	/* microseconds */
    	__u8			read_tput;
    	__u8			read_lat;
    	__u8			write_tput;
    	__u8			write_lat;
    	__le16			idle_power;
    	__u8			idle_scale;
    	__u8			rsvd19;
    	__le16			active_power;
    	__u8			active_work_scale;
    	__u8			rsvd23[9];
    };

    struct nvme_id_ctrl {
            __le16                  vid;
            __le16                  ssvid;
            char                    sn[20];
            char                    mn[40];
            char                    fr[8];
            __u8                    rab;
            __u8                    ieee[3];
            __u8                    cmic;
            __u8                    mdts;
            __le16                  cntlid;
            __le32                  ver;
            __le32                  rtd3r;
            __le32                  rtd3e;
            __le32                  oaes;
            __le32                  ctratt;
            __u8                    rsvd100[156];
            __le16                  oacs;
            __u8                    acl;
            __u8                    aerl;
            __u8                    frmw;
            __u8                    lpa;
            __u8                    elpe;
            __u8                    npss;
            __u8                    avscc;
            __u8                    apsta;
            __le16                  wctemp;
            __le16                  cctemp;
            __le16                  mtfa;
            __le32                  hmpre;
            __le32                  hmmin;
            __u8                    tnvmcap[16];
            __u8                    unvmcap[16];
            __le32                  rpmbs;
            __u8                    rsvd316[4];
            __le16                  kas;
            __u8                    rsvd322[190];
            __u8                    sqes;
            __u8                    cqes;
            __le16                  maxcmd;
            __le32                  nn;
            __le16                  oncs;
            __le16                  fuses;
            __u8                    fna;
            __u8                    vwc;
            __le16                  awun;
            __le16                  awupf;
            __u8                    nvscc;
            __u8                    rsvd531;
            __le32                  sgls;
            __le16                  acwu;
            __u8                    rsvd534[2];
            __u8                    rsvd540[228];
            char                    subnqn[256];
            __u8                    rsvd1024[768];
            __le32                  ioccsz;
            __le32                  iorcsz;
            __le16                  icdoff;
            __u8                    ctrattr;
            __u8                    msdbd;
            __u8                    rsvd1804[244];
            struct nvme_id_power_state      psd[32];
            __u8                    vs[1024];
    };


    struct nvme_smart_log {
            __u8                    critical_warning;
            __u8                    temperature[2];
            __u8                    avail_spare;
            __u8                    spare_thresh;
            __u8                    percent_used;
            __u8                    rsvd6[26];
            __u8                    data_units_read[16];
            __u8                    data_units_written[16];
            __u8                    host_reads[16];
            __u8                    host_writes[16];
            __u8                    ctrl_busy_time[16];
            __u8                    power_cycles[16];
            __u8                    power_on_hours[16];
            __u8                    unsafe_shutdowns[16];
            __u8                    media_errors[16];
            __u8                    num_err_log_entries[16];
            __le32                  warning_temp_time;
            __le32                  critical_comp_time;
            __le16                  temp_sensor[8];
            __le32                  thm_temp1_trans_count;
            __le32                  thm_temp2_trans_count;
            __le32                  thm_temp1_total_time;
            __le32                  thm_temp2_total_time;
            __u8                    rsvd232[280];
    };
    
    #define NVME_IOCTL_ID           _IO('N', 0x40)
    #define NVME_IOCTL_ADMIN_CMD    _IOWR('N', 0x41, struct nvme_admin_cmd)
    #define NVME_IOCTL_SUBMIT_IO    _IOW('N', 0x42, struct nvme_user_io)
    //////////////////////////////////////////////////////


    static int nvme_submit_admin_passthru(int fd, struct nvme_passthru_cmd *cmd)
    {
    	return ioctl(fd, NVME_IOCTL_ADMIN_CMD, cmd);
    }

    #define NVME_IDENTIFY_DATA_SIZE 4096
    int nvme_identify13(int fd, __u32 nsid, __u32 cdw10, __u32 cdw11, void *data)
    {
    	struct nvme_admin_cmd cmd;
        memset(&cmd, 0, sizeof(cmd));
        cmd.opcode = 0x06; //nvme_admin_identify;
        cmd.addr = (uint64_t) data;
        cmd.nsid = nsid;
        cmd.cdw10 = cdw10;
        cmd.cdw11 = cdw11;
        // cmd.data_len = NVME_IDENTIFY_DATA_SIZE;
        cmd.data_len = sizeof(cmd);

    	return nvme_submit_admin_passthru(fd, &cmd);
    }
    
    int nvme_identify(int fd, __u32 nsid, __u32 cdw10, void *data)
    {
    	return nvme_identify13(fd, nsid, cdw10, 0, data);
    }
    
    int nvme_identify_ctrl(int fd, void *data)
    {
    	return nvme_identify(fd, 0, 1, data);
    }


    bool is_kvssd(int fd) {
        struct nvme_id_ctrl ctrl;
        memset(&ctrl, 0, sizeof (struct nvme_id_ctrl));
        int err = nvme_identify_ctrl(fd, &ctrl);
        if (err) {
            // fprintf(stderr, "ERROR : nvme_identify_ctrl() failed 0x%x\n", err);
            return false;
        }

        // this may change based on firmware revision change
        // check firmware version
        // sample FW Revision ETA50K24, letter K at index 5 is the key
        if (strlen((char *) ctrl.fr) >= 6 && *((char *)ctrl.fr + 5) == 'K') {
            // fprintf(stderr, "found a kv device FW: %s\n", ctrl.fr);
            return true;
        }

        // fprintf(stderr, "not a kv device\n");
        return false;
    }

struct nvme_passthru_cmd* create_pt(int opcode, int nsid, size_t data_len, int cdw10)
{
    struct nvme_passthru_cmd *pt = NULL;
    char *data = NULL;
    
    pt = (nvme_passthru_cmd *)malloc(sizeof(struct nvme_passthru_cmd));
    if (pt == NULL) {
       printf("Failed to allocate memory for passthru structure\n");
        exit(-1);
    }
    
    data = (char *)calloc(1, data_len);
    if (data == NULL) {
        printf("Failed to allocate memory for data\n");
        exit(-1);
    }

    pt->opcode = opcode;
    pt->nsid = nsid;
    pt->addr = (__u64)data;
    pt->data_len = data_len;
    pt->cdw10 = cdw10;

    return pt;
}

void identify_ns_nvme(int fd, struct nvme_passthru_cmd *pt)
{
    char *data = (char*)pt->addr;
    __u64 namespace_size = 0;
    __u64 namespace_utilization = 0;
    int ret = ioctl(fd, NVME_IOCTL_ADMIN_CMD, pt);
    if (ret != 0) {
        printf("NVMe Identify Namespace IOCTL Failed %d\n", ret);
        return;
    }

    printf("IDENTIFY NAMESPACE DETAILS (assumes 512B/sector)\n");
    namespace_size = *((__u64*)data);
    printf("Namespace Size:             %lld sectors \n", namespace_size);
    printf("Namespace Size (KB):        %lld\n", (namespace_size * 512) / 1000);
    printf("Namespace Capacity:         %lld sectors\n", *((__u64*)(&data[8])));
    printf("Namespace Capacity (KB):    %lld\n", (*((__u64*)(&data[8])) * 512) / (1000));
    namespace_utilization = *((__u64*)(&data[16]));

    if (is_kvssd(fd)) {
        printf("Namespace Utilization:      %.2f\%\n", 1.0 * namespace_utilization/10000.0 * 100);
        printf("KV Namespace Utilization (KB): %lld\n", (namespace_utilization * namespace_size / 10000L * 512) / 1000);
    } else {
        // printf("Namespace Utilization:      %.2f\%\n", 1.0 * namespace_utilization/100.0);
        printf("BLOCK Namespace Utilization (KB): %lld\n", (namespace_utilization * 512 ) / 1000);
    }
    printf("\n");
}

void smart_nvme(int fd, struct nvme_passthru_cmd *pt)
{
    char *data = (char*)pt->addr;
    int ret = ioctl(fd, NVME_IOCTL_ADMIN_CMD, pt);
    if (ret != 0) {
        printf("NVMe SMART IOCTL Failed %d\n", ret);
        return;
    }

    printf("SMART DETAILS\n");
    printf("Temperature:                %d\n", *((__u16*)(data+1)));
    printf("Available spare space:      %d\n", *((__u8*)(&data[3])));
    printf("Available spare threshold:  %d\n", *((__u8*)(&data[4])));
    printf("Percentage used:            %d\n", *((__u8*)(&data[5])));
    printf("Data Units Read:         %llu\n", *((__u64*)&data[32]));
    printf("Data Units Read:         %llu\n", *((__u64*)&data[40]));
    printf("Data Units Written:         %llu\n", *((__u64*)&data[48]));
    printf("Data Units Written:         %llu\n", *((__u64*)&data[56]));
    printf("\n");
}

int main(int argc, char* argv[])
{
    int fd;
    int ret;
    struct nvme_passthru_cmd *pt = NULL;
    
    if (argc < 2) {
        printf("Missing args\n");
        return 1;
    }
    
    fd = open(argv[1], O_RDWR);
    if (fd == 0) {
        printf("Failed to open device file\n");
        return 1;
    } else {
        printf("Device file opened\n");
    }

    //Identify namespace
    pt = create_pt(0x06, 1, 4096, 0);
    if (pt) {
        identify_ns_nvme(fd, pt);
        free((void*)pt->addr);
        free(pt);
    }
    
    pt = create_pt(0x02, 0xffffffff, 512, 0x007f0002);
    if (pt) {
        smart_nvme(fd, pt);
        free((void*)pt->addr);
        free(pt);
    }

    close(fd);

    return 0;
}
