/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.utils.install;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Random;

public class PasswordGenerator {

	
	private int minimumPasswordLength = 8 ;
	
	private int maximumPasswordLength = 12 ;
	
	private boolean isExpectedNumberic = true ;
	
	private boolean isExpectedBothCase = true ;
	
	private static final ArrayList<Character> alphaLetters = new ArrayList<Character>() ;

	private static final ArrayList<Character> alphaUpperLetters = new ArrayList<Character>() ;

	private static final ArrayList<Character> numericLetters = new ArrayList<Character>() ;
	
	
	static {
		for(int x = 'a' ; x <= 'z' ; x++) {
			char v = (char)x ;
			alphaLetters.add(Character.toLowerCase(v)) ;
			alphaUpperLetters.add(Character.toUpperCase(v)) ;
		}
		for(int i = 0 ; i < 10 ; i++) {
			numericLetters.add(Character.forDigit(i,10)) ;
 		}
	}
	

	
	public static void main(String[] args) {
		PasswordGenerator pg = new PasswordGenerator() ;
		System.out.println(pg.generatorPassword()) ;
	}
	
	
	private int getPasswordLength() {
		int ret = 0;
		
		if (minimumPasswordLength == maximumPasswordLength) {
			ret = minimumPasswordLength ;
		}
		else {
			
			int diff = Math.abs(maximumPasswordLength - minimumPasswordLength) + 1 ;
			ret = minimumPasswordLength + new Random().nextInt(diff) ;
		}
		return (ret) ;
	}
	
	
	public String generatorPassword() {
	
		String password = null ;
		
		ArrayList<Character> all = new ArrayList<Character>() ;
		
		all.addAll(alphaLetters) ;
		all.addAll(alphaUpperLetters) ;
		all.addAll(numericLetters) ;
 				
		int len = getPasswordLength() ;
		
		SecureRandom random = new SecureRandom() ;
		
		int setSz = all.size();
		
		do
		{
			StringBuilder sb = new StringBuilder();
			
			for(int i = 0 ; i < len ; i++) {
				int index = random.nextInt(setSz) ;
				Character c = all.get(index) ;
				while ((i == 0) && Character.isDigit(c)) {
					index = random.nextInt(setSz) ;
					c = all.get(index) ;
				}
				sb.append(all.get(index)) ;
			}
			password = sb.toString() ;
		} while (! isValidPassword(password)) ;
		
		
		return password ;
		
	}
	
	private boolean isValidPassword(String pass) {
		boolean ret = true ;
		
		if (isExpectedNumberic || isExpectedBothCase) {
			boolean lowerCaseFound = false ;
			boolean digitFound = false ;
			boolean upperCaseFound = false ;
			for(char c : pass.toCharArray()) {
				if (!digitFound && Character.isDigit(c)) {
					digitFound = true ;
				}
				else if (!lowerCaseFound && Character.isLowerCase(c)) {
					lowerCaseFound = true ;
				}
				else if (!upperCaseFound && Character.isUpperCase(c) ) {
					upperCaseFound = true ;
				}
			}
			
			if (isExpectedNumberic && !digitFound) {
				ret = false  ;
			}
			
			if (isExpectedBothCase && (!lowerCaseFound || !upperCaseFound)) {
				ret = false ;
			}
		}
		
		return ret ;
	}
}
