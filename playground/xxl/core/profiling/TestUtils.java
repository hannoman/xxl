package xxl.core.profiling;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestUtils {

	private static String resolveFilename(String fileName) throws FileNotFoundException {
		String result;
		
		String testdata_dirname = "temp_data";
//		 System.out.println("Trying to resolve to: \""+"<project dir>\\"+ testdata_dirname +"\\"+ fileName + "\"");

		// and the whole thing in short
		Path curpath = Paths.get("").toAbsolutePath();
		if (!curpath.resolve(testdata_dirname).toFile().exists()) {
			throw new FileNotFoundException("Error: Couldn't find \"" + testdata_dirname + "\" directory.");
		}
		result = curpath.resolve(testdata_dirname).resolve(fileName).toString();
		System.out.println("resolved to: \"" + result + "\".");
		return result;
	}

}
