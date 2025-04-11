// sort.c

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#define BUFFER_SIZE 4096  // Número de inteiros a serem lidos por vez
#define MIN_TEMP -1000    // Temperatura mínima esperada (ajuste conforme necessário)
#define MAX_TEMP 1000     // Temperatura máxima esperada (ajuste conforme necessário)
#define RANGE (MAX_TEMP - MIN_TEMP + 1)

int string_length(const char *str) {
    int len = 0;
    while (str[len]) {
        len++;
    }
    return len;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        const char *error_msg = "Uso: ./sort <sensor_data_file> <region>\n";
        int len = string_length(error_msg);
        write(STDERR_FILENO, error_msg, len);
        _exit(1);
    }

    char *sensor_data_file = argv[1];
    int region_id = 0;
    for (char *p = argv[2]; *p; p++) {
        region_id = region_id * 10 + (*p - '0');
    }

    int fd = open(sensor_data_file, O_RDWR);
    if (fd == -1) {
        const char *error_msg = "Erro ao abrir o arquivo de dados.\n";
        int len = string_length(error_msg);
        write(STDERR_FILENO, error_msg, len);
        _exit(1);
    }

    // Ler o cabeçalho
    int num_regions = 0;
    int num_region_records = 0;

    if (read(fd, &num_regions, sizeof(int)) != sizeof(int) ||
        read(fd, &num_region_records, sizeof(int)) != sizeof(int)) {
        const char *error_msg = "Erro ao ler o cabeçalho do arquivo.\n";
        int len = string_length(error_msg);
        write(STDERR_FILENO, error_msg, len);
        close(fd);
        _exit(1);
    }

    if (region_id < 1 || region_id > num_regions) {
        const char *error_msg = "Região inválida.\n";
        int len = string_length(error_msg);
        write(STDERR_FILENO, error_msg, len);
        close(fd);
        _exit(1);
    }

    // Calcular o deslocamento para a região desejada
    off_t region_offset = sizeof(int) * 2 + sizeof(int) * num_region_records * (region_id - 1);

    // Inicializar o array de contagem
    int counts[RANGE];
    for (int i = 0; i < RANGE; i++) {
        counts[i] = 0;
    }

    int buffer[BUFFER_SIZE];
    int total_records = num_region_records;
    int records_read = 0;
    off_t current_offset = region_offset;

    // Ler os dados da região em blocos e atualizar as contagens
    while (records_read < total_records) {
        int to_read = BUFFER_SIZE;
        if (total_records - records_read < BUFFER_SIZE) {
            to_read = total_records - records_read;
        }

        // Ler os dados do arquivo
        ssize_t bytes_read = pread(fd, buffer, to_read * sizeof(int), current_offset);
        if (bytes_read == -1 || bytes_read != (ssize_t)(to_read * sizeof(int))) {
            const char *error_msg = "Erro ao ler os dados da região.\n";
            int len = string_length(error_msg);
            write(STDERR_FILENO, error_msg, len);
            close(fd);
            _exit(1);
        }

        // Atualizar as contagens
        for (int i = 0; i < to_read; i++) {
            int temp = buffer[i];
            if (temp < MIN_TEMP || temp > MAX_TEMP) {
                const char *error_msg = "Valor fora do intervalo.\n";
                int len = string_length(error_msg);
                write(STDERR_FILENO, error_msg, len);
                close(fd);
                _exit(1);
            }
            counts[temp - MIN_TEMP]++;
        }

        current_offset += bytes_read;
        records_read += to_read;
    }

    // Escrever os dados ordenados de volta no arquivo
    current_offset = region_offset;

    for (int i = 0; i < RANGE; i++) {
        int count = counts[i];
        if (count > 0) {
            int temp = i + MIN_TEMP;
            for (int j = 0; j < count; j++) {
                ssize_t bytes_written = pwrite(fd, &temp, sizeof(int), current_offset);
                if (bytes_written == -1 || bytes_written != sizeof(int)) {
                    const char *error_msg = "Erro ao escrever os dados ordenados.\n";
                    int len = string_length(error_msg);
                    write(STDERR_FILENO, error_msg, len);
                    close(fd);
                    _exit(1);
                }
                current_offset += bytes_written;
            }
        }
    }

    close(fd);

    return 0;
}

