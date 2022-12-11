import mido


class NoteRecord:
  def __init__(self):
    self.notes_on = {}
    self.note_record = {}


def main():
  with open('debussy_DEB_CLAI.MID', 'rb') as test_midi:
    mido_file = mido.MidiFile(file=test_midi)
    for i, track in enumerate(mido_file.tracks):
      print('Track {}: {}'.format(i, track.name))
      for msg in track:
        if isinstance(msg, mido.midifiles.meta.MetaMessage):
          if msg.type == 'track_name':
            print('track_name {}'.format(msg.name))
          elif msg.type == 'number':
            print('number {}'.format(msg.value))
          elif msg.type == 'copyright':
            print('copyright {}'.format(msg.text))
          elif msg.type == 'instrument_name':
            print('instrument_name {}'.format(msg.name))
          elif msg.type == 'text':
            print('copyright {}'.format(msg.text))
          elif msg.type == 'key_signature':
            print('key_signature {}'.format(msg.key))
          elif msg.type == 'time_signature':
            print('time_signature {}/{} time_per_32={}'.format(
              msg.numerator, msg.denominator, msg.notated_32nd_notes_per_beat))


if __name__ == "__main__":
  main()