# midi_learner

This may be a little overkill but I'm looking to learn about some features in google cloud.

This project will do a few things.

## Make a manifest

Take data in google cloud and make a list of all files.

Put this into a bigquery table.

## Encode each note

Using dataflow.  Each not will be encoded into a one hot style encoding, in this fashion.

C0 C#0 D0 D#0 Etc

0.5 .06 0 0 Etc

Each decimal value represents the velocity of each note. In
order to keep it consistent we'll make sure to use the tempo
of the song and go up to 32 notes.

## Take the encoded notes and build a predictive model

We'll build a predictive model using either pytorch or tensorflow.

The model will predict which note will come next.



