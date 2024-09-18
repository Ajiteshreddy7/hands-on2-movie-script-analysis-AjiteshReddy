package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split each line into two parts: the character and their dialogue
        String line = value.toString();
        String[] parts = line.split(":", 2); // Split by the first colon to get character name and dialogue

        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            // Tokenize the dialogue to count words
            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            int wordCountInDialogue = tokenizer.countTokens();

            // Set character name as the key and word count as the value
            character.set(characterName);
            wordCount.set(wordCountInDialogue);

            // Emit character and word count
            context.write(character, wordCount);
        }
    }
}
