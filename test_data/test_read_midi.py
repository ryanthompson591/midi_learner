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
        print(msg)


if __name__ == "__main__":
  main()