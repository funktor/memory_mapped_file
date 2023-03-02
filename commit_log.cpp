#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <random>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <unistd.h>
#include <mutex>
#include <thread>
#include <ctime> 
#include <sys/stat.h>   // stat
#include <stdbool.h>    // bool type
#include <fstream>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>

#define handle_error(msg) \
    do { perror(msg); exit(EXIT_FAILURE); } while (0)

// Maximum length of mmap'd file 100KB
#define MAX_MMAP_LENGTH 1024*100

// Check if file exists or not
bool file_exists (const char *filename) {
    struct stat   buffer;   
    return (stat (filename, &buffer) == 0);
}

// Generate random string of max_length
std::string generate(int max_length){
    std::string possible_characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 engine(rd());
    std::uniform_int_distribution<> dist(0, possible_characters.size()-1);
    std::string ret = "";

    for(int i = 0; i < max_length; i++){
        int random_index = dist(engine); //get index between 0 and possible_characters.size()-1
        ret += possible_characters[random_index];
    }
    return ret;
}

std::string get_current_time() {
    auto clock = std::chrono::system_clock::now();
    std::time_t clock_time = std::chrono::system_clock::to_time_t(clock);
    char *curr_time_char = std::ctime(&clock_time);
    std::string curr_time(curr_time_char, curr_time_char + strlen(curr_time_char) - 1);
    return curr_time;
}

class CommitLogMemoryMap {
    public:

    std::string base_filename = "commit-log-mmap.txt";
    std::vector<std::vector<long>> positions;
    long file_id = -1;
    std::vector<long> pos_to_file_id;
    std::vector<long> local_pos;
    long curr_local_pos = 0;
    std::vector<std::string> files;
    std::vector<char*> mmap_objs;

    std::mutex m;

    CommitLogMemoryMap() {
        init();
    }

    void init() {
        // Current file id following file rotation policy
        file_id += 1;

        // Filename of current file
        std::string str_filename = std::to_string(file_id) + "-" + base_filename;
        const char* filename = str_filename.c_str();

        files.push_back(str_filename);
        int fd;

        // Create file if not exists
        if (file_exists(filename)) {
            fd = open(filename, O_RDWR, 0644);
        }
        else {
            fd = open(filename, O_RDWR | O_CREAT, 0644);
        }

        if (fd == -1) {
            handle_error("Error opening file for writing");
        }

        // For mmap we need to stretch file
        if (lseek(fd, MAX_MMAP_LENGTH, SEEK_SET) == -1) {
            close(fd);
            handle_error("Error getting to end of file");
        }

        // After stretching write 1 byte at the end
        if (write(fd, "", 1) == -1) {
            close(fd);
            handle_error("Error writing last byte of the file");
        }

        // Create mmap object
        char *mmap_obj = (char *) mmap(NULL, MAX_MMAP_LENGTH, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            
        if (mmap_obj == MAP_FAILED)
            handle_error("mmap");

        // Add the mmap object for current file
        mmap_objs.push_back(mmap_obj);

        close(fd);

        // positions[i][j] refers to the end byte position in file id 'i' and line number 'j'
        std::vector<long> new_positions;
        positions.push_back(new_positions);
    }

    void create_mmap_objs() {
        for (long i = 0; i < files.size(); i++) {
            // Create file if not exists
            const char *filename = files[i].c_str();
            int fd = open(filename, O_RDWR, 0644);

            if (fd == -1) {
                handle_error("Error opening file for writing");
            }

            // Create mmap object
            char *mmap_obj = (char *) mmap(NULL, MAX_MMAP_LENGTH, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                
            if (mmap_obj == MAP_FAILED)
                handle_error("mmap");

            // Add the mmap object for current file
            if (i < mmap_objs.size()) {
                mmap_objs[i] = mmap_obj;
            }
            else {
                mmap_objs.push_back(mmap_obj);
            }

            close(fd);
        }

    }

    void writefile(std::vector<std::string> messages, long start=__LONG_MAX__) {
        long start_pos, end_pos;

        {
            std::lock_guard<std::mutex> lock (m);

            // start line number (absolute line number not relative to file)
            start = (start < local_pos.size()) ? start:local_pos.size();

            if (start < local_pos.size()) {
                // start line number is not the current EOF position
                // then truncate all data after start line number.

                long curr_file_id = file_id;
                long g = local_pos.size();

                file_id = pos_to_file_id[start];
                curr_local_pos = local_pos[start];

                long j = curr_file_id;

                while (j > file_id) {
                    // Remove all positions after file_id
                    positions.pop_back();

                    // Unmap all files after file_id
                    if (j < mmap_objs.size())
                        munmap(mmap_objs[j], MAX_MMAP_LENGTH);

                    std::string str_filename = std::to_string(j) + "-" + base_filename;
                    const char* filename = str_filename.c_str();

                    // Delete all files after file_id
                    if (remove(filename) != 0)
                        handle_error("Error deleting file");

                    j--;
                }

                j = local_pos.size()-1;

                while (j >= start && pos_to_file_id.size() > 0 && local_pos.size() > 0) {
                    // Update pos_to_file_id and local_pos by removing all
                    // absolute line numbers after start.
                    long fid = pos_to_file_id.back();
                    long lpos = local_pos.back();
                    
                    // Remove all line numbers in file_id after start from positions
                    if (file_id < positions.size() 
                        && fid == file_id 
                        && lpos >= curr_local_pos) {

                        positions[file_id].pop_back();
                    }
                    
                    pos_to_file_id.pop_back();
                    local_pos.pop_back();
                    j--;
                }

                // For current file_id, replace truncated data with null chars
                if (file_id < positions.size() && file_id < mmap_objs.size()) {
                    long s = positions[file_id].size() > 0 ? positions[file_id].back():0;
                    for (long j = s; j < MAX_MMAP_LENGTH; j++) {
                        mmap_objs[file_id][j] = '\0';
                    }
                }
            }

            long i = 0;

            while (i < messages.size()) {
                std::string message = messages[i];

                // Add timestamp to log message
                message = get_current_time() + " - " + message + "\n";

                // Get start and end byte positions in the file
                start_pos = (file_id < positions.size() && curr_local_pos > 0) ? positions[file_id][curr_local_pos-1]:0;
                end_pos = start_pos + message.length();
                
                if (end_pos > MAX_MMAP_LENGTH) {
                    // Current file will exceed MAX LENGTH after inserting message.
                    // Create new file

                    // Reset line number within current file
                    curr_local_pos = 0;

                    init();
                }
                else {
                    // File size is safe to write.

                    // Write the message into mmap'd region
                    long j = 0;
                    char *mmap_obj = mmap_objs[file_id];

                    // Write message to memory mapped region
                    for (size_t k = start_pos; k < end_pos; k++) {
                        mmap_obj[k] = message[j++];
                    }

                    // Update positions vector with new entries and add entries
                    // if required
                    if (curr_local_pos < positions[file_id].size()) {
                        if (curr_local_pos > 0) positions[file_id][curr_local_pos] = positions[file_id][curr_local_pos-1] + message.length();
                        else positions[file_id][curr_local_pos] = message.length();
                    }
                    else {
                        if (positions[file_id].size() > 0) positions[file_id].push_back(positions[file_id].back() + message.length());
                        else positions[file_id].push_back(message.length());
                    }

                    // Update mapping from absolute line number to file id.
                    pos_to_file_id.push_back(file_id);

                    // Update mapping from absolute line number to local line number.
                    local_pos.push_back(curr_local_pos);

                    // Increment local line number
                    curr_local_pos += 1;
                    i += 1;
                }
            }
        }
    }

    void readfile(std::string &output, long start, long end=__LONG_MAX__) {
        
        {
            std::lock_guard<std::mutex> lock (m);

            while (1) {
                // start and end line numbers absolute
                start = (start < local_pos.size()) ? start:local_pos.size();
                end = (end < local_pos.size()-1) ? end:local_pos.size()-1;

                if (end < start || start < 0 || end < 0) {
                    output = "";
                    break;
                }

                // Get file id corresponding to absolute start and end line numbers
                long file_id_start = pos_to_file_id[start];
                long file_id_end = pos_to_file_id[end];

                long start_pos, end_pos;

                if (file_id_end > file_id_start) {
                    // start and end line numbers are in different files
                    start_pos = 0;

                    if (file_id_start < positions.size() 
                        && local_pos[start] > 0 
                        && (local_pos[start]-1) < positions[file_id_start].size()) 

                        start_pos = positions[file_id_start][local_pos[start]-1];
                    else {
                        handle_error("Invalid starting byte position encountered");
                    }
                    
                    // end position is the last byte position of current file
                    if (file_id_start < positions.size() 
                        && positions[file_id_start].size() > 0)

                        end_pos = positions[file_id_start].back();
                    else {
                        handle_error("Invalid ending byte position encountered");
                    }

                    char *mmap_obj = mmap_objs[file_id_start];
                    // Read from mmap'd region
                    std::string str(mmap_obj + start_pos, mmap_obj + end_pos);

                    output += str;
                    // Update start line number to first line of next file in sequence
                    start += positions[file_id_start].size()-local_pos[start];
                }

                else {
                    // start and end line numbers are in same file
                    start_pos = 0;
                    if (local_pos[start] > 0) start_pos = positions[file_id_start][local_pos[start]-1];
                    end_pos = positions[file_id_start][local_pos[end]];
                
                    char *mmap_obj = mmap_objs[file_id_start];
                    // Read from mmap'd region
                    std::string str(mmap_obj + start_pos, mmap_obj + end_pos);

                    output += str;
                    break;
                }
            }
        }
    }
};

namespace boost {
    namespace serialization {
        // When the class Archive corresponds to an output archive, the
        // & operator is defined similar to <<.  Likewise, when the class Archive
        // is a type of input archive the & operator is defined similar to >>.
        template<class Archive>
        void serialize(Archive & a, 
            CommitLogMemoryMap & cmlog, const unsigned int version)
        {
            // & operator acts as input/output to/from archive a
            a 
            & cmlog.base_filename 
            & cmlog.positions
            & cmlog.file_id 
            & cmlog.pos_to_file_id 
            & cmlog.local_pos
            & cmlog.curr_local_pos
            & cmlog.files;
        }
    }
}

// Serialize CommitLogMemoryMap object into file.
void serialize(CommitLogMemoryMap &cmlog, const std::string filename) {
    std::ofstream outfile(filename);
    boost::archive::text_oarchive archive(outfile);

    // write cmlog to archive
    archive << cmlog;
}

// Deserialize from file to CommitLogMemoryMap object
void deserialize(CommitLogMemoryMap &cmlog, const std::string filename) {
    std::ifstream infile(filename);
    boost::archive::text_iarchive archive(infile);

    // read from archive to cmlog
    archive >> cmlog;

    // initialize the memory mappings
    cmlog.create_mmap_objs();
}

void log_data(long num_strings=10000, long str_size=10, 
                long batch_size=10, std::string ser_file="log.dat") {

    CommitLogMemoryMap log;

    if (file_exists(ser_file.c_str())) {
        // Deserialize the file into object
        deserialize(log, ser_file);
    }

    std::vector<std::string> data;
    std::vector<std::thread> my_threads;

    long i;

    // Generate random strings
    for (i = 0; i < num_strings; i++) {
        std::string h = generate(str_size);
        data.push_back(h);
    }

    auto start = std::chrono::high_resolution_clock::now();
    
    // Each batch is handled within a different thread
    for (i = 0; i < num_strings; i+=batch_size) {
        std::vector<std::string> subdata(data.begin()+i, data.begin()+i+batch_size);
        my_threads.push_back(move(std::thread(&CommitLogMemoryMap::writefile, std::ref(log), subdata, __LONG_MAX__)));
    }

    // Wait for all threads to finish tasks
    for (i = 0; i < my_threads.size(); i++) {
        my_threads[i].join();
    }
    
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;

    // Serailize the object
    serialize(log, ser_file);
}

void read_data(long start, long end, std::string ser_file="log.dat") {
    CommitLogMemoryMap log;

    // Deserialize the file into object
    deserialize(log, ser_file);

    std::string output = "";
    log.readfile(output, start, end);
    std::cout << output << std::endl;
}

void update(long num_strings=10000, long str_size=10, long start=0, std::string ser_file="log.dat") {
    CommitLogMemoryMap log;

    std::vector<std::string> data;
    long i;

    // Generate random strings
    for (i = 0; i < num_strings; i++) {
        std::string h = generate(str_size);
        data.push_back(h);
    }
    
    deserialize(log, ser_file);
    log.writefile(data, start);
}

int main(int argc, char *argv[]) {
    log_data(100000, 20, 10000);
    read_data(101, 110);
    update(5000, 30, 2000);
}