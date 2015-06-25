#include <iostream>
#include <string>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>

#define FILES_TO_WRITE 10000
#define BUFFER_SIZE 1048576
#define FILE_SIZE 10485760
#define THREADS_COUNT 8

char *FILE_PREFIX = "/mnt/lustre/testfile";

struct RunContext {
	int last_file;
	int current_file;
	off_t current_pos;
	RunContext():last_file(0), current_file(0), current_pos(0) {};
};
std::string get_current_file_name(RunContext *ctxt)
{
	std::ostringstream oss;
	oss << FILE_PREFIX << "-" << ctxt << "-" << ctxt->current_file;
	return oss.str();
}
std::string get_last_file_name(RunContext *ctxt)
{
	std::ostringstream oss;
	oss << FILE_PREFIX << "-" << ctxt << "-" << ctxt->last_file;
	return oss.str();	
}
class Buffer {
	char *buf;
	int size;
	char pos2char(off_t pos) { return (pos % 23) + 'A'; }
public:
	Buffer(int s) : size(s) {
		buf = new char[s];
	}
	~Buffer() { delete[] buf; }
	char *get() { return buf; }
	void fillAt(off_t pos);
	bool checkAt(off_t pos);
};
void Buffer::fillAt(off_t pos)
{
	for (int i = 0; i < size; i++)
		buf[i] = pos2char(pos + i);
}
bool Buffer::checkAt(off_t pos)
{
	for (int i = 0; i < size; i++)
		if (buf[i] != pos2char(pos + i)) return false;
	return true;
}

void check_file_content(const std::string &fname)
{
	int fd = open(fname.c_str(), O_RDONLY);
	assert(fd >= 0);
	Buffer rbuf(BUFFER_SIZE);
	for (off_t pos = 0; pos < FILE_SIZE; pos += BUFFER_SIZE) {
		size_t count = BUFFER_SIZE;
		off_t start = pos;
		char *buf = rbuf.get();
		while (count > 0) {
			ssize_t ret = pread(fd, buf, count, start);
			if (ret > 0) {
				buf += ret;
				start += ret;
				count -= ret;
			} else {
				std::cout << "Read ERROR: " << ret << ", "
					  << errno << std::endl;
			}
		}
		assert(rbuf.checkAt(pos));
	}
	close(fd);
	std::cout << fname << " is checked correctly." << std::endl;
}

void context_gc(RunContext *ctxt)
{
	int removed = 0;
	while (removed < 3 && ctxt->last_file < ctxt->current_file) {
		std::string fname = get_last_file_name(ctxt);
		check_file_content(fname);
		int ret = truncate(fname.c_str(), 0);
		if (unlink(fname.c_str()) == -1 || ret == -1) {
			std::cout << "Truncate/Unlink ERROR: " << errno
				  << std::endl;
		}
		ctxt->last_file++;
		removed++;
	}
}

void *run_test(void *arg)
{
	RunContext *ctxt = (RunContext *)arg;
	Buffer wbuf(BUFFER_SIZE);
	while (ctxt->current_file < FILES_TO_WRITE) {
		std::string fname = get_current_file_name(ctxt);
		int fd;
		fd = open(fname.c_str(), O_WRONLY | O_CREAT, 0666);
		while (fd < 0 && errno == ENOSPC) {
			context_gc(ctxt);
			fd = open(fname.c_str(), O_WRONLY | O_CREAT, 0666);
		}
		if (fd < 0) {
			std::cout << "Failed to create file: " << errno
				  << std::endl;
			return NULL;
		}
		wbuf.fillAt(ctxt->current_pos);
		char *buf = wbuf.get();
		size_t count = BUFFER_SIZE;
		off_t pos = ctxt->current_pos;
		while (count > 0) {
		        ssize_t ret = pwrite(fd, buf, count, pos);
			if (ret > 0) {
				buf += ret;
				pos += ret;
				count -= ret;
			} else if (ret == 0 || errno == ENOSPC) {
				context_gc(ctxt);
			} else {
				std::cout << "Write ERROR: " << errno
					  << std::endl;
				return NULL;
			}
		}
		close(fd);
		ctxt->current_pos += BUFFER_SIZE;
		if (ctxt->current_pos >= FILE_SIZE) {
			ctxt->current_pos = 0;
			ctxt->current_file++;
		}
	}
	return NULL;
}

int main(int argc, char *argv[])
{
	if (argc == 2 && argv[1][0] == '/') {
		std::cout << "Write to " << argv[1] << std::endl;
	} else if (argc == 1) {
		std::cout << "Write to default mount point " << FILE_PREFIX
			  << std::endl;
	} else {
		std::cout << "Usage: ./a.out /path/to/lustre/filename"
			  << std::endl;
	}
	pthread_t tids[THREADS_COUNT];
	for (int i = 0; i < THREADS_COUNT; i++) {
		// It leaks, I know..
		pthread_create(&tids[i], NULL, run_test, new RunContext());
	}
	for (int i = 0; i < THREADS_COUNT; i++) {
		pthread_join(tids[i], NULL);
	}
	return 0;
}
