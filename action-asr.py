#!/usr/bin/env python3
import os, json, queue, struct, threading, time, toml
import paho.mqtt.client as mqtt
from google.cloud import speech
from google.cloud.speech import enums
from google.cloud.speech import types
from pprint import pprint
try:
    import configparser as configparser
except ImportError:
    import ConfigParser as configparser

CONFIG_INI = "config.ini"

Config = configparser.ConfigParser()
if not os.path.exists(CONFIG_INI):
    shutil.copyfile(CONFIG_INI + '.default', CONFIG_INI)
Config.read(CONFIG_INI)

GOOGLE_CREDENTIALS = Config.get('global', 'google_credentials')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_CREDENTIALS

LANG = Config.get('global', 'language')
TOML_PATH = '/etc/snips.toml'
TOML = toml.load(TOML_PATH)

# Get config values from /etc/snips.toml.
try:
    MQTT_ADDR_PORT = TOML['snips-common']['mqtt']
    MQTT_ADDR, MQTT_PORT = MQTT_ADDR_PORT.split(':')
except (KeyError, ValueError):
    MQTT_ADDR = 'localhost'
    MQTT_PORT = 1883
    MQTT_ADDR_PORT = "{}:{}".format(MQTT_ADDR, str(MQTT_PORT))

try:
    CONFIG_SITES = TOML['snips-asr-google']['audio']
except (KeyError, ValueError):
    CONFIG_SITES = ['default@mqtt']

try:
    MQTT_USER = TOML['snips-common']['mqtt_username']
    MQTT_PASS = TOML['snips-common']['mqtt_password']
except (KeyError, ValueError):
    MQTT_USER = ''
    MQTT_PASS = ''

SITES = {}
for site in CONFIG_SITES:
    siteAddress = site.split("@")
    SITES[siteAddress[0]] = {}
startListeningTopic = 'hermes/asr/startListening'
stopListeningTopic = 'hermes/asr/stopListening'


class Transcoder(object):
    """
    Converts audio chunks to text
    """

    def __init__(self, encoding, rate, language, site_id):
        self.buff = queue.Queue()
        self.encoding = encoding
        self.language = language
        self.rate = rate
        self.closed = True
        self.confidence = None
        self.transcript = None
        self.seconds = None
        self.site_id = site_id

    def start(self):
        """Start up streaming speech call"""
        threading.Thread(target=self.process).start()

    def response_loop(self, responses):
        """
        Pick up the final result of Speech to text conversion
        """
        for response in responses:
            if not response.results:
                continue
            result = response.results[0]
            if not result.alternatives:
                continue
            confidence = result.alternatives[0].confidence
            transcript = result.alternatives[0].transcript
            if result.is_final:
                self.confidence = confidence
                self.transcript = transcript
                self.seconds = time.time() - SITES[self.site_id]['start_time']
                text_captured(self.transcript, self.confidence, self.seconds, self.site_id, SITES[self.site_id]['sessionId'])
                stop_listening(self.site_id)

    def process(self):
        """
        Audio stream recognition and result parsing
        """
        print("Processing audio...")
        client = speech.SpeechClient()
        config = types.RecognitionConfig(
            encoding=self.encoding,
            sample_rate_hertz=self.rate,
            language_code=self.language
        )
        streaming_config = types.StreamingRecognitionConfig(
            config=config,
            interim_results=False,
            single_utterance=False)
        audio_generator = self.stream_generator()
        requests = (types.StreamingRecognizeRequest(audio_content=content)
                    for content in audio_generator)

        responses = client.streaming_recognize(streaming_config, requests)
        try:
            self.response_loop(responses)
        except:
            # self.start()
            print("Audio processing stopped")
            return

    def stream_generator(self):
        while not self.closed:
            chunk = self.buff.get()
            if chunk is None:
                return
            data = [chunk]
            while True:
                try:
                    chunk = self.buff.get(block=False)
                    if chunk is None:
                        return
                    data.append(chunk)
                except queue.Empty:
                    break
            yield b''.join(data)

    def write(self, data):
        """
        Writes data to the buffer
        """
        self.buff.put(data)

    def join(self, timeout=None):
        """ Stop the thread. """
        self._stopevent.set()
        threading.Thread.join(self, timeout)


def on_connect(client, userdata, flags, rc):
    print('Connected to MQTT')
    for site_id in SITES.keys():
        print('Subscribing site "' + site_id + '"')
        SITES[site_id]['listening'] = False
        SITES[site_id]['recording'] = False
        mqtt.subscribe('hermes/audioServer/' + site_id + '/audioFrame')
    mqtt.subscribe(startListeningTopic)
    mqtt.subscribe(stopListeningTopic)


def on_message(client, userdata, msg):
    if msg.topic.endswith("audioFrame"):
        capture_frame(msg)
    if msg.topic == startListeningTopic:
        payload = (json.loads(msg.payload))
        site_id = payload['siteId']
        session_id = payload['sessionId']
        start_listening(site_id, session_id)
    if msg.topic == stopListeningTopic:
        payload = (json.loads(msg.payload))
        site_id = payload['siteId']
        stop_listening(site_id)


def capture_frame(msg):
    topic = msg.topic.split("/")
    site_id = topic[2]

    if not SITES[site_id]['listening']:
        return

    seconds = time.time() - SITES[site_id]['start_time']
    if seconds >= 5:
        print("Listen timeout")
        text_captured("", 1, seconds, site_id, SITES[site_id]['sessionId'])
        stop_listening(site_id)

    # print("Recording data from site " + site)

    riff, size, fformat = struct.unpack('<4sI4s', msg.payload[:12])
    if riff != b'RIFF':
        print("RIFF parse error")
        return
    if fformat != b'WAVE':
        print("FORMAT parse error")
        return
    # print("size: %d" % size)

    if not SITES[site_id]['recording']:
        start_recording(site_id)

    chunkOffset = 52
    while chunkOffset < size:
        subchunk2id, subchunk2size = struct.unpack('<4sI', msg.payload[chunkOffset:chunkOffset + 8])
        chunkOffset += 8
        if subchunk2id == b'data':
            if SITES[site_id]['recording']:
                SITES[site_id]['transcoder'].write(msg.payload[chunkOffset:chunkOffset + subchunk2size])
        chunkOffset = chunkOffset + subchunk2size + 8


def start_listening(site_id, sessionId):
    if SITES[site_id]['listening']:
        return
    print("Listen start on site " + site_id)
    SITES[site_id]['start_time'] = time.time()
    SITES[site_id]['listening'] = True
    SITES[site_id]['sessionId'] = sessionId
    SITES[site_id]['transcoder'] = Transcoder(
        encoding=enums.RecognitionConfig.AudioEncoding.LINEAR16,
        rate=16000,
        language=LANG,
        site_id=site_id
    )
    SITES[site_id]['transcoder'].start()
    SITES[site_id]['transcoder'].closed = False


def stop_listening(site_id):
    if not SITES[site_id]['listening']:
        return
    print("Listen stop on site " + site_id)
    stop_recording(site_id)
    SITES[site_id]['listening'] = False
    SITES[site_id]['transcoder'].closed = True
    SITES[site_id]['transcoder'].join()

def start_recording(site_id):
    if SITES[site_id]['recording']:
        return
    print("Record start on site " + site_id)
    SITES[site_id]['recording'] = True


def stop_recording(site_id):
    if not SITES[site_id]['recording']:
        return
    print("Record stop on site " + site_id)
    SITES[site_id]['recording'] = False


def query(sessionId, input):
    data = {'sessionId': sessionId, 'input': input}
    json_data = json.dumps(data)
    mqtt.publish('hermes/nlu/query', str(json_data))


def text_captured(text, likelihood, seconds, siteId, sessionId):
    print('Captured text "' + text + '" with confidence ' + str(likelihood))
    data = {'sessionId': sessionId, 'text': text, 'likelihood': likelihood, 'seconds': seconds, 'siteId': siteId}
    json_data = json.dumps(data)
    mqtt.publish('hermes/asr/textCaptured', str(json_data))


def main():
    mqtt.on_connect = on_connect
    mqtt.on_message = on_message
    mqtt.username_pw_set(MQTT_USER, MQTT_PASS)
    mqtt.connect(MQTT_ADDR, int(MQTT_PORT))
    mqtt.loop_forever()


if __name__ == "__main__":
    mqtt = mqtt.Client()
    main()
