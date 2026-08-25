/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.architecture.rules;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import org.junit.jupiter.api.Nested;

import java.util.Arrays;
import java.util.List;

import static com.tngtech.archunit.core.domain.JavaModifier.ABSTRACT;
import static com.tngtech.archunit.core.domain.JavaModifier.PRIVATE;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;
import static org.apache.flink.architecture.common.GivenJavaClasses.javaClassesThat;

/**
 * Rules ensuring nested test classes are actually executed.
 *
 * <p>Surefire's default excludes drop {@code **}{@code /*$*}, so nested class files are never
 * scanned by the unit test run; JUnit Jupiter only auto-discovers {@code @Nested} classes, which
 * must be non-static inner classes. A {@code static} nested class carrying test methods therefore
 * never runs, with no build signal indicating the omission.
 */
public class NestedTestClassRules {

    /** JUnit 5 and (for modules still mid-migration) JUnit 4 test method annotations. */
    private static final List<String> TEST_METHOD_ANNOTATIONS =
            Arrays.asList(
                    "org.junit.jupiter.api.Test",
                    "org.junit.jupiter.api.TestTemplate",
                    "org.junit.jupiter.api.RepeatedTest",
                    "org.junit.jupiter.api.TestFactory",
                    "org.junit.jupiter.params.ParameterizedTest",
                    "org.junit.Test");

    /**
     * A class JUnit would execute if it were reachable: it declares or inherits a test method.
     * {@code getAllMethods()} covers inherited methods, e.g. a variant class that only extends a
     * base and adds no annotation itself.
     */
    private static final DescribedPredicate<JavaClass> ARE_EXECUTABLE_TEST_CLASSES =
            DescribedPredicate.describe(
                    "are executable JUnit test classes",
                    clazz ->
                            clazz.getAllMethods().stream()
                                    .anyMatch(
                                            method ->
                                                    TEST_METHOD_ANNOTATIONS.stream()
                                                            .anyMatch(method::isAnnotatedWith)));

    @ArchTest
    public static final ArchRule STATIC_NESTED_TEST_CLASSES_SHOULD_BE_INNER_CLASSES =
            freeze(
                            javaClassesThat()
                                    .areMemberClasses()
                                    .and()
                                    .doNotHaveModifier(ABSTRACT)
                                    .and(ARE_EXECUTABLE_TEST_CLASSES)
                                    .should()
                                    .beInnerClasses())
                    // not every module has nested test classes
                    .allowEmptyShould(true)
                    .as(
                            "A concrete nested test class must be a non-static inner class "
                                    + "(annotated with @Nested); surefire's default excludes and "
                                    + "JUnit Jupiter's discovery rules both silently skip a static "
                                    + "nested test class, so it never runs. Abstract static bases "
                                    + "for @Nested variants are unaffected by this rule.");

    @ArchTest
    public static final ArchRule NESTED_TEST_CLASSES_SHOULD_NOT_BE_PRIVATE =
            freeze(
                            javaClassesThat()
                                    .areAnnotatedWith(Nested.class)
                                    .should()
                                    .notHaveModifier(PRIVATE))
                    // not every module has @Nested test classes
                    .allowEmptyShould(true)
                    .as("A @Nested test class must not be private; JUnit cannot instantiate it");
}
