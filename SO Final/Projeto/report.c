// report.c

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdlib.h>

typedef struct region_stats {
    int region_id;
    int median;
    float average;
    int max;
    int min;
} region_stats;

int string_length(const char *str) {
    int len = 0;
    while (str[len]) {
        len++;
    }
    return len;
}

void int_to_string(int value, char *str) {
    int len = 0;
    int temp_value = value;
    char temp_str[16];
    int is_negative = 0;

    if (value < 0) {
        is_negative = 1;
        temp_value = -temp_value;
    }

    if (temp_value == 0) {
        temp_str[len++] = '0';
    } else {
        while (temp_value > 0) {
            temp_str[len++] = '0' + (temp_value % 10);
            temp_value /= 10;
        }
    }

    int pos = 0;
    if (is_negative) {
        str[pos++] = '-';
    }

    // Inverter a string
    for (int i = len - 1; i >= 0; i--) {
        str[pos++] = temp_str[i];
    }
    str[pos] = '\0';
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        const char *error_msg = "Uso: ./report <num_regions>\n";
        int len = string_length(error_msg);
        write(STDERR_FILENO, error_msg, len);
        _exit(1);
    }

    int num_regions = 0;
    for (char *p = argv[1]; *p; p++) {
        if (*p < '0' || *p > '9') {
            const char *error_msg = "Número de regiões inválido.\n";
            int len = string_length(error_msg);
            write(STDERR_FILENO, error_msg, len);
            _exit(1);
        }
        num_regions = num_regions * 10 + (*p - '0');
    }

    // Nome padrão do arquivo de dados
    char *sensor_data_file = "sensor_data.bin";

    // Alocar memória para o array de estatísticas
    region_stats *stats_array = (region_stats *)malloc(num_regions * sizeof(region_stats));
    if (stats_array == NULL) {
        const char *error_msg = "Erro ao alocar memória.\n";
        int len = string_length(error_msg);
        write(STDERR_FILENO, error_msg, len);
        _exit(1);
    }

    pid_t pids[num_regions];
    int pipefd[2];
    if (pipe(pipefd) == -1) {
        const char *error_msg = "Erro ao criar pipe.\n";
        int len = string_length(error_msg);
        write(STDERR_FILENO, error_msg, len);
        free(stats_array);
        _exit(1);
    }

    for (int region = 1; region <= num_regions; region++) {
        pid_t pid = fork();
        if (pid == -1) {
            const char *error_msg = "Erro ao criar processo filho.\n";
            int len = string_length(error_msg);
            write(STDERR_FILENO, error_msg, len);
            free(stats_array);
            _exit(1);
        } else if (pid == 0) {
            // Processo filho
            close(pipefd[0]);  // Fecha o lado de leitura no filho

            // Redireciona stdout para o lado de escrita do pipe
            dup2(pipefd[1], STDOUT_FILENO);
            close(pipefd[1]);

            // Preparar argumentos para o stats
            char region_str[16];
            int_to_string(region, region_str);

            // Executar o programa stats
            execlp("./stats", "./stats", sensor_data_file, region_str, "stdout", NULL);

            // Se execlp retornar, ocorreu um erro
            const char *error_msg = "Erro ao executar stats.\n";
            int len = string_length(error_msg);
            write(STDERR_FILENO, error_msg, len);
            _exit(1);
        } else {
            // Processo pai
            pids[region - 1] = pid;
            // Continua para a próxima região
        }
    }

    // Fecha o lado de escrita do pipe no pai
    close(pipefd[1]);

    // Ler estatísticas do pipe
    int regions_read = 0;
    ssize_t read_bytes;
    while (regions_read < num_regions) {
        // Ler uma estrutura region_stats do pipe
        region_stats stats;
        size_t total_read = 0;
        char *stats_ptr = (char *)&stats;
        while (total_read < sizeof(region_stats)) {
            read_bytes = read(pipefd[0], stats_ptr + total_read, sizeof(region_stats) - total_read);
            if (read_bytes == -1) {
                const char *error_msg = "Erro ao ler do pipe.\n";
                int len = string_length(error_msg);
                write(STDERR_FILENO, error_msg, len);
                free(stats_array);
                _exit(1);
            } else if (read_bytes == 0) {
                // EOF alcançado prematuramente
                const char *error_msg = "EOF inesperado ao ler do pipe.\n";
                int len = string_length(error_msg);
                write(STDERR_FILENO, error_msg, len);
                free(stats_array);
                _exit(1);
            }
            total_read += (size_t)read_bytes;
        }

        // Armazenar as estatísticas no array
        stats_array[regions_read] = stats;
        regions_read++;
    }

    close(pipefd[0]);

    // Esperar por todos os processos filhos
    for (int i = 0; i < num_regions; i++) {
        int status;
        waitpid(pids[i], &status, 0);
    }

    // Processar estatísticas agregadas
    int highest_region = stats_array[0].region_id;
    int lowest_region = stats_array[0].region_id;
    int highest_value = stats_array[0].max;
    int lowest_value = stats_array[0].min;
    float highest_avg = stats_array[0].average;
    float lowest_avg = stats_array[0].average;
    int highest_avg_region = stats_array[0].region_id;
    int lowest_avg_region = stats_array[0].region_id;

    for (int i = 1; i < num_regions; i++) {
        if (stats_array[i].max > highest_value) {
            highest_value = stats_array[i].max;
            highest_region = stats_array[i].region_id;
        }
        if (stats_array[i].min < lowest_value) {
            lowest_value = stats_array[i].min;
            lowest_region = stats_array[i].region_id;
        }
        if (stats_array[i].average > highest_avg) {
            highest_avg = stats_array[i].average;
            highest_avg_region = stats_array[i].region_id;
        }
        if (stats_array[i].average < lowest_avg) {
            lowest_avg = stats_array[i].average;
            lowest_avg_region = stats_array[i].region_id;
        }
    }

    // Imprimir o relatório

    char num_str[16];
    int len;

    // Mensagem: A Região X registou o valor mais alto: Y °C;
    write(STDOUT_FILENO, "A Região  ", 10);
    int_to_string(highest_region, num_str);
    len = string_length(num_str);
    write(STDOUT_FILENO, num_str, len);
    write(STDOUT_FILENO, " registou o valor mais alto:", 29);
    int_to_string(highest_value, num_str);
    len = string_length(num_str);
    write(STDOUT_FILENO, num_str, len);
    write(STDOUT_FILENO, "°C;\n", 5);

    // Mensagem: A Região X registou o valor mais baixo: Y °C;
    write(STDOUT_FILENO, "A Região  ", 10);
    int_to_string(lowest_region, num_str);
    len = string_length(num_str);
    write(STDOUT_FILENO, num_str, len);
    write(STDOUT_FILENO, " registou o valor mais baixo:", 30);
    int_to_string(lowest_value, num_str);
    len = string_length(num_str);
    write(STDOUT_FILENO, num_str, len);
    write(STDOUT_FILENO, "°C;\n", 5);

    // Mensagem: A Região X registou o valor médio máximo: Y.Y °C;
    write(STDOUT_FILENO, "A Região  ", 10);
    int_to_string(highest_avg_region, num_str);
    len = string_length(num_str);
    write(STDOUT_FILENO, num_str, len);
    write(STDOUT_FILENO, " registou o valor médio máximo:", 33);
    // Converter highest_avg para string com uma casa decimal
    int inteiro = (int)highest_avg;
    int decimal = (int)(highest_avg * 10) % 10;
    if (decimal < 0) decimal = -decimal;
    int_to_string(inteiro, num_str);
    len = string_length(num_str);
    write(STDOUT_FILENO, num_str, len);
    write(STDOUT_FILENO, ".", 1);
    char decimal_char = '0' + decimal;
    write(STDOUT_FILENO, &decimal_char, 1);
    write(STDOUT_FILENO, "°C;\n", 5);

    // Mensagem: A Região X registou o valor médio mínimo: Y.Y °C;
    write(STDOUT_FILENO, "A Região  ", 10);
    int_to_string(lowest_avg_region, num_str);
    len = string_length(num_str);
    write(STDOUT_FILENO, num_str, len);
    write(STDOUT_FILENO, " registou o valor médio mínimo:", 33);
    // Converter lowest_avg para string com uma casa decimal
    inteiro = (int)lowest_avg;
    decimal = (int)(lowest_avg * 10) % 10;
    if (decimal < 0) decimal = -decimal;
    int_to_string(inteiro, num_str);
    len = string_length(num_str);
    write(STDOUT_FILENO, num_str, len);
    write(STDOUT_FILENO, ".", 1);
    decimal_char = '0' + decimal;
    write(STDOUT_FILENO, &decimal_char, 1);
    write(STDOUT_FILENO, "°C;\n\n", 7);
    
    // Imprimir estatísticas para cada região
    for (int i = 0; i < num_regions; i++) {
        	write(STDOUT_FILENO, "Região  ", 8);
        	int_to_string(stats_array[i].region_id, num_str);
        	len = string_length(num_str);
        	write(STDOUT_FILENO, num_str, len);
        	write(STDOUT_FILENO, ":\n", 2);

        // Imprimir * Valor médio: X.X ºC
        	write(STDOUT_FILENO, "Valor médio: ", 15);
        	float avg = stats_array[i].average;
        	inteiro = (int)avg;
        	decimal = (int)(avg * 10) % 10;
        	if (decimal < 0) decimal = -decimal;
        	int_to_string(inteiro, num_str);
        	len = string_length(num_str);
        	write(STDOUT_FILENO, num_str, len);
        	write(STDOUT_FILENO, ".", 1);
        	char avg_decimal_char = '0' + decimal;
        	write(STDOUT_FILENO, &avg_decimal_char, 1);
        	write(STDOUT_FILENO, "ºC\n", 4);

        // Imprimir * Mediana: X ºC
        	write(STDOUT_FILENO, "Mediana: ", 10);
        	int_to_string(stats_array[i].median, num_str);
        	len = string_length(num_str);
        	write(STDOUT_FILENO, num_str, len);
        	write(STDOUT_FILENO, "ºC\n", 4);

        // Imprimir * Valor máximo: X ºC
        	write(STDOUT_FILENO, "Valor máximo: ", 15);
        	int_to_string(stats_array[i].max, num_str);
        	len = string_length(num_str);
        	write(STDOUT_FILENO, num_str, len);
        	write(STDOUT_FILENO, "ºC\n", 4);

        // Imprimir * Valor mínimo: X ºC
        	write(STDOUT_FILENO, "Valor mínimo: ", 15);
        	int_to_string(stats_array[i].min, num_str);
        	len = string_length(num_str);
        	write(STDOUT_FILENO, num_str, len);
        	write(STDOUT_FILENO, "ºC\n\n", 6);
    	}

    // Liberar memória alocada
    free(stats_array);

    return 0;
}

