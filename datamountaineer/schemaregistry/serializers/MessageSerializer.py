try:
  from fastavro.reader import read_data
  from fastavro import dump
  HAS_FAST = True
except:
  HAS_FAST = False

import io
import struct
import avro.io

from datamountaineer.schemaregistry.serializers import SerializerError
from datamountaineer.schemaregistry.client import ClientError

MAGIC_BYTE = 0


class ContextBytesIO(io.BytesIO):

    """
    Wrapper to allow use of BytesIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class MessageSerializer(object):

    """
    A helper class that can serialize and deserialize messages
    that need to be encoded or decoded using the schema registry.

    All encode_* methods return a buffer that can be sent to kafka.
    All decode_* methods expect a buffer received from kafka.
    """

    def __init__(self, registry_client, fast_avro=True):
        self.registry_client = registry_client
        self.id_to_decoder_func = { }
        self.id_to_writers = { }
        self.fast_avro = HAS_FAST

    def _set_subject(self, subject, is_key=False):
        subject_suffix = ('-key' if is_key else '-value')
        # get the latest schema for the subject
        return (subject + subject_suffix)

    def encode_record_with_schema(self, topic, schema, record, is_key=False):
        """
        Given a parsed avro schema, encode a record for the given topic.  The
        record is expected to be a dictionary.

        The schema is registered with the subject of 'topic-value'
        """
        if not isinstance(record, dict):
            raise SerializerError("record must be a dictionary")

        subject = self._set_subject(topic, is_key)

        # register it
        try:
            schema_id = self.registry_client.register(subject, schema)
        except:
            schema_id = None

        if not schema_id:
            message = "Unable to retrieve schema id for subject %s" % (subject)
            raise SerializerError(message)

        if not self.fast_avro:
            self.id_to_writers[schema_id] = avro.io.DatumWriter(schema)

        return self.encode_record_with_schema_id(schema_id, schema, record)

    # subject = topic + suffix
    def encode_record_for_topic(self, topic, record, is_key=False):
        """
        Encode a record for a given topic.

        This is expensive as it fetches the latest schema for a given topic.
        """
        if not isinstance(record, dict):
            raise SerializerError("record must be a dictionary")

        subject = self._set_subject(topic, is_key)

        try:
            schema_id, schema, version = self.registry_client.get_latest_schema(subject)
        except ClientError as e:
            message = "Unable to retrieve schema id for subject %s" % (subject)
            raise SerializerError(message)
        else:
            if not self.fast_avro:
                self.id_to_writers[schema_id] = avro.io.DatumWriter(schema)
            return self.encode_record_with_schema_id(schema_id, schema, record)

    def encode_record_with_schema_id(self, schema_id, schema, record):
        """
        Encode a record with a given schema id.  The record must
        be a python dictionary.
        """
        if not isinstance(record, dict):
            raise SerializerError("record must be a dictionary")

        if not self.fast_avro:
            if schema_id not in self.id_to_writers:
                # get the writer + schema
                try:
                    schema = self.registry_client.get_by_id(schema_id)
                    if not schema:
                        raise SerializerError("Schema does not exist")
                    self.id_to_writers[schema_id] = avro.io.DatumWriter(schema)
                except ClientError as e:
                    raise SerializerError("Error fetching schema from registry")

        with ContextBytesIO() as outf:
            # write the header
            # magic byte
            outf.write(struct.pack('b', MAGIC_BYTE))
            # write the schema ID in network byte order (big end)
            outf.write(struct.pack('>I', schema_id))
            if self.fast_avro:
                dump(outf, record, schema.to_json())
            else:
                writer = self.id_to_writers[schema_id]
                encoder = avro.io.BinaryEncoder(outf)
                writer.write(record, encoder)
            return outf.getvalue()

    def get_schema(self, schema_id):
        # fetch from schema reg
        try:
            # first call will cache in the client
            schema = self.registry_client.get_by_id(schema_id)
        except:
            schema = None

        if not schema:
            err = "unable to fetch schema with id %d" % (schema_id)
            raise SerializerError(err)

        return schema

    # Decoder support
    def _get_decoder_func(self, schema_id, payload):
        if schema_id in self.id_to_decoder_func:
            return self.id_to_decoder_func[schema_id]

        schema = self.get_schema(schema_id)

        curr_pos = payload.tell()

        if self.fast_avro:
            # try to use fast avro
            try:
                schema_dict = schema.to_json()
                payload.seek(curr_pos)
                decoder_func = lambda p: read_data(p, schema_dict)
                self.id_to_decoder_func[schema_id] = decoder_func
                return self.id_to_decoder_func[schema_id]
            except:
                payload.seek(curr_pos)
                pass

        avro_reader = avro.io.DatumReader(schema)

        def decoder(p):
            bin_decoder = avro.io.BinaryDecoder(p)
            return avro_reader.read(bin_decoder)

        self.id_to_decoder_func[schema_id] = decoder
        return self.id_to_decoder_func[schema_id]

    def decode_message(self, message):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.
        """
        if len(message) <= 5:
            raise SerializerError("message is too small to decode")

        with ContextBytesIO(message) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")
            decoder_func = self._get_decoder_func(schema_id, payload)
            return decoder_func(payload)
