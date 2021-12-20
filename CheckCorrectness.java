import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.io.IOException;

public class CheckCorrectness{

	private static final String SINGLE_WORD = "test_cases/mapreduce_single/WordCount/";
	private static final String SINGLE_WORD_ANSWER = "design_doc/output/milestone1/WordCount/";

	private static final String SINGLE_COUNT = "test_cases/mapreduce_single/CountSameLengthWords/";
	private static final String SINGLE_COUNT_ANSWER = "design_doc/output/milestone1/CountSameLengthWords/";

	private static final String SINGLE_VOWELS = "test_cases/mapreduce_single/Vowels/";
	private static final String SINGLE_VOWELS_ANSWER = "design_doc/output/milestone1/Vowels/";

	private static final String MULTI_WORD = "test_cases/mapreduce_multi/WordCount/";
	private static final String MULTI_WORD_ANSWER = "design_doc/output/milestone2/WordCount/";

	private static final String MULTI_COUNT = "test_cases/mapreduce_multi/CountSameLengthWords/";
	private static final String MULTI_COUNT_ANSWER = "design_doc/output/milestone2/CountSameLengthWords/";

	private static final String MULTI_VOWELS = "test_cases/mapreduce_multi/Vowels/";
	private static final String MULTI_VOWELS_ANSWER = "design_doc/output/milestone2/Vowels/";

	private static final String WITH_FAULT_WORD = "test_cases/mapreduce_with_fault_tolerance/WordCount/";
	private static final String WITH_FAULT_WORD_ANSWER = "design_doc/output/milestone3/WordCount/";

	private static final String WITH_FAULT_COUNT = "test_cases/mapreduce_with_fault_tolerance/CountSameLengthWords/";
	private static final String WITH_FAULT_COUNT_ANSWER = "design_doc/output/milestone3/CountSameLengthWords/";

	private static final String WITH_FAULT_VOWELS = "test_cases/mapreduce_with_fault_tolerance/Vowels/";
	private static final String WITH_FAULT_VOWELS_ANSWER = "design_doc/output/milestone3/Vowels/";

	private static List<String> getFilesContents(Path folderPath) {
		
		List<String> fileNames = new ArrayList<>();
		List<String> results = new ArrayList<>();

		// retrieve a list of the files in the folder
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(folderPath)) {
            for (Path path : directoryStream) {
				if (path.toString().contains("output"))
					fileNames.add(path.toString());
					//System.out.println("filenames: "+fileNames);
            }
        } catch (IOException ex) {
            System.err.println("Error reading files");
            ex.printStackTrace();
		}
		
		// go through the list of files
        for (String file : fileNames) {
            try {
                results.addAll(Files.readAllLines(folderPath.resolve(file)));           
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

		return results;
	}



	public static void main(String[] args) throws IOException {

		String currentDirectory = System.getProperty("user.dir");   //get current directory

		// -- Check Correctness of Single Process Behavior --
		Path single_word = Paths.get(currentDirectory+"/"+SINGLE_WORD);
		Path single_word_answer = Paths.get(currentDirectory+"/"+SINGLE_WORD_ANSWER);

		Path single_count = Paths.get(currentDirectory+"/"+SINGLE_COUNT);
		Path single_count_answer = Paths.get(currentDirectory+"/"+SINGLE_COUNT_ANSWER);

		Path single_vowels = Paths.get(currentDirectory+"/"+SINGLE_VOWELS);
		Path single_vowels_answer = Paths.get(currentDirectory+"/"+SINGLE_VOWELS_ANSWER);

		if (getFilesContents(single_word).containsAll(getFilesContents(single_word_answer))
			&& getFilesContents(single_count).containsAll(getFilesContents(single_count_answer))
			&& getFilesContents(single_vowels).containsAll(getFilesContents(single_vowels_answer)))
			System.out.println("Checked correctness of single process behavior...");
		
		
		// -- Check Correctness of Multi Processes Behavior --
		Path multi_word = Paths.get(currentDirectory+"/"+MULTI_WORD);
		Path multi_word_answer = Paths.get(currentDirectory+"/"+MULTI_WORD_ANSWER);

		Path multi_count = Paths.get(currentDirectory+"/"+MULTI_COUNT);
		Path multi_count_answer = Paths.get(currentDirectory+"/"+MULTI_COUNT_ANSWER);

		Path multi_vowels = Paths.get(currentDirectory+"/"+MULTI_VOWELS);
		Path multi_vowels_answer = Paths.get(currentDirectory+"/"+MULTI_VOWELS_ANSWER);

		if (getFilesContents(multi_word).containsAll(getFilesContents(multi_word_answer))
			&& getFilesContents(multi_count).containsAll(getFilesContents(multi_count_answer))
			&& getFilesContents(multi_vowels).containsAll(getFilesContents(multi_vowels_answer)))
			System.out.println("Checked correctness of multi processes behavior...");
		
		// -- Check Correctness of Multi Processes with One Fault Tolerance Behavior --
		Path with_fault_word = Paths.get(currentDirectory+"/"+WITH_FAULT_WORD);
		Path with_fault_word_answer = Paths.get(currentDirectory+"/"+WITH_FAULT_WORD_ANSWER);

		Path with_fault_count = Paths.get(currentDirectory+"/"+WITH_FAULT_COUNT);
		Path with_fault_count_answer = Paths.get(currentDirectory+"/"+WITH_FAULT_COUNT_ANSWER);

		Path with_fault_vowels = Paths.get(currentDirectory+"/"+WITH_FAULT_VOWELS);
		Path with_fault_vowels_answer = Paths.get(currentDirectory+"/"+WITH_FAULT_VOWELS_ANSWER);

		if (getFilesContents(with_fault_word).containsAll(getFilesContents(with_fault_word_answer))
			&& getFilesContents(with_fault_count).containsAll(getFilesContents(with_fault_count_answer))
			&& getFilesContents(with_fault_vowels).containsAll(getFilesContents(with_fault_vowels_answer)))
			System.out.println("Checked correctness of multi processes with one fault tolerance behavior...");
		
	}
}