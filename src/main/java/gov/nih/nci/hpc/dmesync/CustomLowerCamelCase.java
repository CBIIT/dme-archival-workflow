package gov.nih.nci.hpc.dmesync;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;

public class CustomLowerCamelCase extends PropertyNamingStrategy.PropertyNamingStrategyBase {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
    public String translate(String input) {
        if (input == null)
            return input; // garbage in, garbage out

        if (!input.isEmpty() && Character.isUpperCase(input.charAt(0)))
            input = input.substring(0, 1).toLowerCase() + input.substring(1);

        return input;
    }
}
