#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <mosquitto.h>

#define BROKER      "127.0.0.1"
#define PORT        1883
#define TOPIC       "campus/S1/environment"
#define MSG_COUNT   1
#define INTERVAL_S  2

struct wetterdaten {
    int message_id;
    char timestamp[30];
    char station_id[10];
    float temperature_c;
    float humidity_pct;
};

struct wetterdaten data1 = {
    .message_id = 1,
    .timestamp = "2024-06-01 12:00:00",
    .station_id = "S1",
    .temperature_c = 22.5,
    .humidity_pct = 55.0
};


int json_serializer(struct wetterdaten* data, char* buffer, size_t len_buffer) {
    if (buffer == NULL || len_buffer < 120) {
        printf("Error: Buffer is NULL or size is too small\n");
        return -1;
    }
    else if (data == NULL) {
        printf("Error: Data is NULL\n");
        return -2;
    }
    snprintf(buffer, len_buffer, "{\"message id\": %d, \"timestamp\": \"%s\", \"station\": \"%s\", \"temperature\": %.1f, \"humidity\": %.1f}", data->message_id, data->timestamp, data->station_id, data->temperature_c, data->humidity_pct);
    return 1;
}


static void on_connect(struct mosquitto *mosq, void *userdata, int rc) {
    if (rc == 0) {
        printf("[publisher] Connected to %s:%d\n", BROKER, PORT);
    } else {
        fprintf(stderr, "[publisher] Connection failed: %s\n",
                mosquitto_connack_string(rc));
        mosquitto_disconnect(mosq);
    }
}

/* Callback: called after each message is published */
static void on_publish(struct mosquitto *mosq, void *userdata, int mid) {
    printf("[publisher] Message %d delivered to broker\n", mid);
}


int mqtt_connect_and_publish(char* topic, int port, const char* host, struct wetterdaten data) {
    if (host == NULL || topic == NULL) {
        printf("Error: Broker or Topic is NULL\n");
        return -1;
    }
    
    srand((unsigned int)time(NULL));

    mosquitto_lib_init();

    struct mosquitto *mosq = mosquitto_new("mqtt-lab-publisher", true, NULL);
    if (!mosq) {
        fprintf(stderr, "[publisher] Failed to create mosquitto instance\n");
        mosquitto_lib_cleanup();
        return 1;
    }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_publish_callback_set(mosq, on_publish);

    int rc = mosquitto_connect(mosq, host, port, 60);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[publisher] Could not connect: %s\n",
                mosquitto_strerror(rc));
        mosquitto_destroy(mosq);
        mosquitto_lib_cleanup();
        return 1;
    }

    /* Start the network loop in background thread */
    mosquitto_loop_start(mosq);

    char payload[256];
    printf("[publisher] Sending %d messages to topic: %s\n\n", MSG_COUNT, topic);

    for (int i = 1; i <= MSG_COUNT; i++) {
        json_serializer(&data, payload, sizeof(payload));
        printf("[publisher] Publishing: %s\n", payload);

        rc = mosquitto_publish(mosq, NULL, topic,
                               (int)strlen(payload), payload, 1, false);
        if (rc != MOSQ_ERR_SUCCESS) {
            fprintf(stderr, "[publisher] Publish error: %s\n",
                    mosquitto_strerror(rc));
        }
        sleep(INTERVAL_S);
    }

    printf("\n[publisher] Done. Disconnecting.\n");
    mosquitto_disconnect(mosq);
    mosquitto_loop_stop(mosq, true);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 0;
}


int main(void) {
    mqtt_connect_and_publish(TOPIC, PORT, BROKER, data1);
    return 0;
    }