// stats.c

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>

typedef struct region_stats {
    int region_id;
    int median;
    float average;
    int max;
    int min;
} region_stats;

#define BUFFER_SIZE 4096  // Número de inteiros a serem lidos por vez

int string_length(const char *str) {
    int len = 0;
    while (str[len]) {
        len++;
    }
    return len;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        const char *error_msg = "Uso: ./stats <sensor_data_file> <region> [stdout]\n";
        int len = string_length(error_msg);
        write(STDERR_FILENO, error_msg, len);
        _exit(1);
    }

    char *sensor_data_file = argv[1];
    int region_id = 0;
    for (char *p = argv[2]; *p; p++) {
        region_id = region_id * 10 + (*p - '0');
    }

    int output_to_stdout = 0;
    if (argc >= 4 && argv[3][0] == 's') {  // Verifica se o terceiro argumento é "stdout"
        output_to_stdout = 1;
    }

    // Executar o programa sort
    pid_t pid = fork();
    if (pid == -1) {
        const char *error_msg = "Erro ao criar processo filho.\n";
        int len = string_length(error_msg);
        write(STDERR_FILENO, error_msg, len);
        _exit(1);
    } else if (pid == 0) {
        // Processo filho
        execlp("./sort", "./sort", sensor_data_file, argv[2], NULL);
        // Se execlp retornar, houve um erro
        const char *error_msg = "Erro ao executar o sort.\n";
        int len = string_length(error_msg);
        write(STDERR_FILENO, error_msg, len);
        _exit(1);
    } else {
        // Processo pai
        int status;
        waitpid(pid, &status, 0);
        if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
            const char *error_msg = "Sort terminou com erro.\n";
            int len = string_length(error_msg);
            write(STDERR_FILENO, error_msg, len);
            _exit(1);
        }
    }

    // Abrir o arquivo binário para leitura dos dados ordenados
    int fd = open(sensor_data_file, O_RDONLY);
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

    // Calcular média, máximo e mínimo
    int min = 0, max = 0;
    long long sum = 0;
    int buffer[BUFFER_SIZE];
    int total_records = num_region_records;
    int records_read = 0;
    off_t current_offset = region_offset;

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

        for (int i = 0; i < to_read; i++) {
            int value = buffer[i];
            sum += value;
            if (records_read == 0 && i == 0) {
                min = max = value;
            } else {
                if (value < min) min = value;
                if (value > max) max = value;
            }
        }

        current_offset += bytes_read;
        records_read += to_read;
    }

    float average = (float)sum / total_records;

    // Calcular a mediana
    int median;
    if (total_records % 2 == 0) {
        // Registos pares
        int mid_pos = total_records / 2;
        int value1 = 0, value2 = 0;
        off_t offset1 = region_offset + (mid_pos - 1) * sizeof(int);
        off_t offset2 = region_offset + mid_pos * sizeof(int);
        if (pread(fd, &value1, sizeof(int), offset1) != sizeof(int) ||
            pread(fd, &value2, sizeof(int), offset2) != sizeof(int)) {
            const char *error_msg = "Erro ao ler valores para a mediana.\n";
            int len = string_length(error_msg);
            write(STDERR_FILENO, error_msg, len);
            close(fd);
            _exit(1);
        }
        median = (value1 + value2) / 2;
    } else {
        // Registos ímpares
        int mid_pos = total_records / 2;
        int value = 0;
        off_t offset = region_offset + mid_pos * sizeof(int);
        if (pread(fd, &value, sizeof(int), offset) != sizeof(int)) {
            const char *error_msg = "Erro ao ler valor para a mediana.\n";
            int len = string_length(error_msg);
            write(STDERR_FILENO, error_msg, len);
            close(fd);
            _exit(1);
        }
        median = value;
    }

    close(fd);

    // Criar a estrutura region_stats
    region_stats stats;
    stats.region_id = region_id;
    stats.average = average;
    stats.median = median;
    stats.max = max;
    stats.min = min;

    if (output_to_stdout) {
        // Escrever a estrutura no stdout
        write(STDOUT_FILENO, &stats, sizeof(region_stats));
    } else {
        // Escrever as estatísticas em um arquivo binário
        char filename[256];
        int pos = 0;
        char prefix[] = "region-";
        char suffix[] = "-stats.bin";
        // Copiar prefixo
        for (int i = 0; prefix[i]; i++) {
            filename[pos++] = prefix[i];
        }
        // Converter region_id para string
        char region_str[16];
        int len = 0;
        int temp_id = region_id;
        char temp_str[16];
        do {
            temp_str[len++] = '0' + (temp_id % 10);
            temp_id /= 10;
        } while (temp_id > 0);
        // Inverter o string
        for (int i = len - 1; i >= 0; i--) {
            region_str[len - 1 - i] = temp_str[i];
        }
        region_str[len] = '\0';
        // Copiar region_str para filename
        for (int i = 0; region_str[i]; i++) {
            filename[pos++] = region_str[i];
        }
        // Copiar sufixo
        for (int i = 0; suffix[i]; i++) {
            filename[pos++] = suffix[i];
        }
        filename[pos] = '\0';

        int fd_out = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_out == -1) {
            const char *error_msg = "Erro ao criar o arquivo de estatísticas.\n";
            int len = string_length(error_msg);
            write(STDERR_FILENO, error_msg, len);
            _exit(1);
        }

        if (write(fd_out, &stats, sizeof(region_stats)) != sizeof(region_stats)) {
            const char *error_msg = "Erro ao escrever as estatísticas.\n";
            int len = string_length(error_msg);
            write(STDERR_FILENO, error_msg, len);
            close(fd_out);
            _exit(1);
        }

        close(fd_out);
    }

    return 0;
}
