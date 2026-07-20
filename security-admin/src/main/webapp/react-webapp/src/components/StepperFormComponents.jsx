/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useState, useRef } from "react";
import { Form } from "react-final-form";
import arrayMutators from "final-form-arrays";
import { Button } from "react-bootstrap";
import usePrompt from "Hooks/usePrompt";

const PromptDialog = ({ isDirtyField, isUnblock }) => {
  usePrompt(
    "Are you sure you want to leave? You have unsaved changes.",
    isDirtyField && !isUnblock
  );
  return null;
};

const StepperForm = ({
  steps,
  onSubmit,
  initialValues = {},
  blockUI,
  preventUnBlock = false,
  /** Called when navigating away (Next, jump to review, or header step click) so the parent
   *  can sync form state (e.g. prune access types after resources change in step 1). */
  onBeforeNavigate
}) => {
  const [currentStep, setCurrentStep] = useState(0);
  const formRef = useRef(null);
  const [formValues, setFormValues] = useState(initialValues);
  const [formKey, setFormKey] = useState(0);
  const [hasUserStarted, setHasUserStarted] = useState(false);

  // True once the user reaches the last step via "Next".
  // Enables clicking step numbers in the header to jump freely.
  const [hasVisitedLastStep, setHasVisitedLastStep] = useState(false);

  // True once the user advances past the Service step (e.g. to Resources).
  // ServiceStep uses this to confirm resets when later-step data may exist.
  const [hasProgressedPastService, setHasProgressedPastService] =
    useState(false);

  const lastStepIndex = steps.length - 1;
  const isLastStep = currentStep === lastStepIndex;
  const currentStepConfig = steps[currentStep];

  // ─── Navigation handlers ──────────────────────────────────────────────────

  const runBeforeNavigateAndGetValues = (toStep) => {
    const form = formRef.current;
    if (onBeforeNavigate && form) {
      onBeforeNavigate({
        fromStep: currentStep,
        toStep,
        form
      });
      return form.getState().values;
    }
    return null;
  };

  const handleNext = (values) => {
    const toStep = isLastStep ? lastStepIndex : currentStep + 1;
    const afterNavigate = runBeforeNavigateAndGetValues(toStep);
    const resolved = afterNavigate !== null ? afterNavigate : values;
    const merged = { ...formValues, ...resolved };
    setFormValues(merged);

    if (isLastStep) {
      setHasUserStarted(false);
      return onSubmit(merged);
    }

    const next = currentStep + 1;
    if (currentStep === 0) {
      setHasProgressedPastService(true);
    }
    if (next === lastStepIndex) {
      setHasVisitedLastStep(true);
    }

    setHasUserStarted(true);

    setCurrentStep(next);
  };

  const handlePrevious = (values, form) => {
    // If user has visited last step, validate current step before going back
    if (hasVisitedLastStep) {
      const currentStepErrors = validate(values);

      // Check if there are validation errors
      if (currentStepErrors && Object.keys(currentStepErrors).length > 0) {
        // Force react-final-form to show validation errors
        if (form) {
          form.getRegisteredFields().forEach((fieldName) => {
            form.blur(fieldName);
          });
        }

        // Show user-friendly message
        alert("Please fill in all required fields before proceeding.");
        return false;
      }
    }

    // If validation passes or user hasn't visited last step, proceed
    setCurrentStep(Math.max(0, currentStep - 1));
    return true;
  };

  // Jump directly to ReviewConfirmStep, but validate current step first.
  const handleJumpToReview = (values, form) => {
    // Validate current step before jumping
    const currentStepErrors = validate(values);

    // Check if there are validation errors
    if (currentStepErrors && Object.keys(currentStepErrors).length > 0) {
      // Force react-final-form to show validation errors
      if (form) {
        form.getRegisteredFields().forEach((fieldName) => {
          form.blur(fieldName);
        });
      }

      // Show user-friendly message
      alert("Please fill in all required fields before proceeding to review.");
      return false;
    }

    // If validation passes, proceed with jump
    const afterNavigate = runBeforeNavigateAndGetValues(lastStepIndex);
    const resolved = afterNavigate !== null ? afterNavigate : values;
    const merged = { ...formValues, ...resolved };
    setFormValues(merged);
    setCurrentStep(lastStepIndex);
    return true;
  };

  // Called when user clicks a step number in the header.
  // Only active after the user has visited the last step at least once.
  const handleStepClick = (index) => {
    if (!hasVisitedLastStep) return;
    if (index === currentStep) return;
    const afterNavigate = runBeforeNavigateAndGetValues(index);
    if (afterNavigate !== null) {
      setFormValues({ ...formValues, ...afterNavigate });
    }
    setCurrentStep(index);
  };

  // Exposed to step components so they can reinitialize the wizard.
  // serviceFieldSnapshot: fields from the Service step to keep after reset
  // (merged on top of initialValues); clears Resources and later-step data.
  const handleResetForm = (serviceFieldSnapshot = {}) => {
    setFormValues({ ...initialValues, ...serviceFieldSnapshot });
    setFormKey((k) => k + 1); // force react-final-form to fully reinitialize
    setCurrentStep(0);
    setHasVisitedLastStep(false);
    setHasProgressedPastService(false);
    setHasUserStarted(false);
  };

  // ─── Validation ───────────────────────────────────────────────────────────

  const validate = (values) => {
    if (currentStepConfig.validate) {
      return currentStepConfig.validate(values);
    }
    return {};
  };

  // ─── Render ───────────────────────────────────────────────────────────────

  return (
    <div className="stepper-form-container">
      {/* ── Stepper Header ── */}
      <div className="stepper-header">
        {steps.map((step, index) => {
          const isActive = index === currentStep;
          const isCompleted = index < currentStep;
          const isClickable = hasVisitedLastStep && index !== currentStep;

          return (
            <React.Fragment key={index}>
              <div
                className={`stepper-item${isActive ? " active" : ""}${isCompleted ? " completed" : ""}${isClickable ? " clickable" : ""}`}
                onClick={() => handleStepClick(index)}
                title={isClickable ? `Jump to ${step.title}` : undefined}
              >
                <div className="stepper-number">{index + 1}</div>
                <div className="stepper-content">
                  <div className="stepper-title">
                    <h6 className="mb-0">{step.title}</h6>
                  </div>
                  <div className="stepper-subtitle">
                    <span className="font-13">{step.subtitle}</span>
                  </div>
                </div>
              </div>

              {index < lastStepIndex && (
                <div
                  className={`stepper-arrow${isCompleted ? " completed" : ""}`}
                >
                  ›
                </div>
              )}
            </React.Fragment>
          );
        })}
      </div>
      <hr />

      {/* ── Form Content ── */}
      <Form
        key={formKey}
        initialValues={formValues}
        validate={validate}
        onSubmit={handleNext}
        mutators={{ ...arrayMutators }}
        keepDirtyOnReinitialize
        render={({ handleSubmit, submitting, values, form, dirty }) => {
          formRef.current = form;
          return (
            <form onSubmit={handleSubmit}>
              <PromptDialog
                isDirtyField={hasUserStarted || dirty}
                isUnblock={preventUnBlock}
              />
              <div className="stepper-body">
                {currentStepConfig.component &&
                  React.createElement(currentStepConfig.component, {
                    values,
                    // react-final-form API (change, reset, batch, etc.) — required by
                    // steps that call form.change(...) outside of Field render props.
                    form,
                    hasProgressedPastService,
                    onResetForm: handleResetForm,
                    ...currentStepConfig.componentProps
                  })}
              </div>

              {/* ── Navigation Buttons ── */}
              <div className="stepper-footer">
                <Button
                  variant="outline-secondary"
                  onClick={() => handlePrevious(values, form)}
                  disabled={currentStep === 0 || submitting || blockUI}
                >
                  Previous
                </Button>

                <div className="d-flex gap-2">
                  {/* "Jump to Review" — shown when user has visited the last step
                    and is on any intermediate step */}
                  {hasVisitedLastStep && !isLastStep && (
                    <Button
                      variant="outline-primary"
                      onClick={() => handleJumpToReview(values, form)}
                      disabled={submitting || blockUI}
                    >
                      <i className="fa fa-forward me-1"></i>
                      Jump to Review (Step {lastStepIndex + 1})
                    </Button>
                  )}

                  <Button
                    variant="primary"
                    type="submit"
                    disabled={submitting || blockUI}
                  >
                    {isLastStep ? "Submit" : "Next"}
                  </Button>
                </div>
              </div>
            </form>
          );
        }}
      />
    </div>
  );
};

export default StepperForm;
