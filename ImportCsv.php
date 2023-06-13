<?php

namespace App\Jobs;

use Webpatser\Uuid\Uuid;
use Illuminate\Bus\Queueable;
use App\Services\CsvImporterService;
use Illuminate\Queue\SerializesModels;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Price\PriceClient;
use App\CsvImportProgress;
use App\CsvImportJob;
use App\CsvImportBatch;
use App\CsvImport;
use App\CsvImportLog;
use App\PriceSession;
use App\Exceptions\CsvImporterException;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Encryption\DecryptException;
use Illuminate\Support\Facades\Crypt;

class ImportCsv implements ShouldQueue, ShouldBeUnique
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    const CSV_RECORD_OFFSET = 5000;
    const CSV_STATUS_DONE = 1;
    const CSV_STATUS_DONE_WITH_FAILURE = 2;
    const CSV_STATUS_DONE_FAILED = 3;
    const IMPORT_STATUS_NOTICE = 2;
    const IMPORT_STATUS_SUCCESS = 1;
    const IMPORT_STATUS_FAILED = 0;
    const PRICE_ERR_INVALID_JSON_FORMAT_RETRY = 69;
    const ERROR_RETRY = 2;
    const PRICE_ERR_INVALID_SESSION = 6;
    const PRICE_ERR_INVALID_NONCE = 7;
    const PRICE_ERR_SESSION_EXPIRED = 9;
    const TEMPORARY_EMAIL_ADDRESS = "DOT_TEMP_EMAIL";
    const PWD_KY = ["p", "a", "s", "s", "w", "o", "r", "d"];
    public $uniqueFor = 3600 * 3; //3hrs
    public $csv_service;
    public $records;
    private $row_num;
    private $loan_resource;
    private $borrower_resource;
    private $process_string;
    private $csv_import_id;
    private $progress;
    private $tries = 0;
    private $forceRetry = false;
    private $client = null;

    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct(
        CsvImporterService $csv_service,
        $records,
        int $csv_import_id,
        int $tries
    ) {
        $this->csv_service = $csv_service;
        $this->records = json_decode($records, true);
        $this->csv_import_id = $csv_import_id;
        $this->tries = $tries;
    }

    public function uniqueId()
    {
        return "{$this->csv_import_id}-{$this->row_num}";
    }

    /**
     * Handles job failure
     *
     * @param $e
     * @return void
     */
    public function failed($e)
    {
        $this->error(
            "Error encountered - " .
                $e->getMessage() .
                ". Retrying if possible."
        );
        $this->csv_service->retry($e);
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        $row_num = $this->csv_service->getRowNumber() + 1;
        // stop processing other rows when a job failed
        if (
            $this->csv_service->csvImport()->status ==
            CsvImport::JOB_STATUS_SUCCESS_WITH_FAILURE
        ) {
            return;
        }

        $this->row_num = $row_num;

        // extract row values
        if (!is_array($this->records)) {
            $this->runAfter();
            return;
        }
        $values = array_values($this->records);
        // extract row headers to validate and check its settings
        foreach (array_keys($this->records) as $index => $header) {
            try {
                $this->csv_service->colHeader($header);
                $result = null;
                if ($this->csv_service->csvImport()->transaction != "update") {
                    // add loans
                    if ($header != "Loan_Number_ID") {
                        $this->csv_service->validateCsv($values[$index]);
                        $result = $this->csv_service->setHeaderDataset(
                            $values[$index]
                        );
                    }
                } else {
                    $this->csv_service->validateCsv($values[$index]);
                    $result = $this->csv_service->setHeaderDataset(
                        $values[$index]
                    );
                }
                if ($result === false) {
                    $csvEx = new CsvImporterException(
                        CsvImporterException::HEADER_NOT_SUPPORTED
                    );
                    $this->csv_service->logImport([
                        "row_num" => $row_num,
                        "col_name" => "Unsupported",
                        "col_headers" => $this->csv_service->getColHeader(),
                        "status" => self::IMPORT_STATUS_FAILED,
                        "description" => $csvEx->getErrorMessage(
                            CsvImporterException::HEADER_NOT_SUPPORTED
                        ),
                    ]);
                }
            } catch (\Exception $e) {
                $this->csv_service->logImport([
                    "row_num" => $row_num,
                    "col_name" => "Unsupported",
                    "col_headers" => $this->csv_service->getColHeader(),
                    "status" => self::IMPORT_STATUS_FAILED,
                    "description" => $e->getMessage(),
                ]);
            }
        }

        try {
            $posibleTradeTransactions = ["NTrade"];

            $this->pingPrice();
            $log = $this->csv_service
                ->csvImport()
                ->logs()
                ->ByImportId($this->csv_import_id)
                ->WhereRowNum($this->row_num)
                ->IsLoanIDNotBlank()
                ->first();

            if (
                in_array(
                    $this->csv_service->csvImport()->transaction,
                    $posibleTradeTransactions
                )
            ) {
                // TradeSetup Transactions

                $this->loginAsBorrower();
                $this->runTradeSetupProcess();
            } else {
                if (!empty($log["loan_id"])) {
                    $this->records["Loan_Number_ID"] = $log["loan_id"];
                }
                $this->setLoanId($this->records);
                $this->loginAsBorrower();

                $loanId = $this->csv_service->price_config->getLoanId();
                if (!empty($loanId)) {
                    CsvImportLog::where("csv_import_id", $this->csv_import_id)
                        ->where("row_num", $this->row_num)
                        ->update([
                            "loan_id" => $loanId,
                        ]);
                }
                $this->runProcess();
            }

            if ($this->forceRetry) {
                throw new \Exception("Error Encountered while processing..");
            }
            $this->runAfter();
        } catch (\Exception $e) {
            $this->failed($e);
            $this->error($e->getMessage());
            if ($this->csv_service->isNewLoan()) {
                $this->endSession();
            } else {
                PriceSession::findByWebSessionId(
                    $this->csv_service->importConfig()->getWebSessionId()
                )->update([
                    "in_used" => 0,
                ]);
            }
        }
    }

    private function loginAsBorrower()
    {
        if ($this->csv_service->isNewLoan()) {
            $job = new CsvImportJob();
            $result = $job
                ->where("csv_import_id", $this->csv_import_id)
                ->where("row_num", $this->row_num - 1)
                ->first();
            $emailAddress = $this->getEmailAddress();
            $pwd = "";
            if (!is_null($result) && !empty($result->email_tmp)) {
                $pwd = Crypt::decryptString($result->email_tmp);
            }
            $this->endSession();
            $this->csv_service->price_config->credentials(
                $emailAddress,
                $pwd,
                true
            );
        }
    }

    private function clearBorrowerTemp()
    {
        if ($this->csv_service->isNewLoan()) {
            $resource = new \Price\Resources\Borrowers(
                $this->csv_service->price_config
            );
            $borrowers = $resource->get();
            if (is_array($borrowers) && count($borrowers) > 0) {
                $fields = [
                    "document_" . implode("", self::PWD_KY) => "",
                ];
                $emailAddress = $this->getEmailAddress();
                if ($borrowers[0]->EmailAddress === $emailAddress) {
                    $fields["email_address"] = "";
                }
                $resource
                    ->setResourceId($borrowers[0]->PersonID)
                    ->update($fields);
            }
        }
    }

    /**
     * Ping PRICE
     *
     * @return void
     */
    private function pingPrice()
    {
        $this->client = PriceClient::create($this->csv_service->price_config);
        $response = $this->client->post("PING_SESSION", []);
        if (
            !$response->Successful &&
            str_contains($response->ErrorMessage, "Invalid session")
        ) {
            if (!$this->csv_service->isNewLoan()) {
                PriceSession::findByWebSessionId(
                    $this->csv_service->importConfig()->getWebSessionId()
                )->delete();
            }
            throw new \Exception($response->ErrorMessage);
        }
    }

    private function endSession()
    {
        try {
            if (!is_null($this->client)) {
                $this->client->post("END_SESSION", []);
            }
        } catch (\Exception $e) {
            $this->error(
                "[DOT][QUEUE] Error ending session. " . $e->getMessage()
            );
        }
    }

    private function runAfter()
    {
        try {
            $job = $this->csv_service->getJob()->first();
            if (!is_null($job)) {
                CsvImportJob::where("job_id", $job->job_id)->update([
                    "tries" => $job->tries + 1,
                    "is_processed" => CsvImportJob::STATUS_IS_PROCESSED,
                    "payload" => null,
                    "email_tmp" => null,
                    "last_run_at" => \DB::raw("NOW()"),
                ]);
                $batch = CsvImportBatch::find($job->batch_id);
                $batch->rows_processed = $this->csv_service->totalProcessed();
                if (
                    $batch->rows_per_batch <=
                    CsvImportJob::numberOfProcessedInBatch($job->batch_id)
                ) {
                    $batch_seq = [
                        1 => 10,
                        10 => 30,
                        30 => 30,
                    ];
                    $batch->rows_per_batch = isset(
                        $batch_seq[$batch->rows_per_batch]
                    )
                        ? $batch_seq[$batch->rows_per_batch]
                        : 30;
                }
                $batch->queue_name = CsvImporterService::getQueueName();
                $batch->save();
            }
            $this->clearBorrowerTemp();
            if ($this->csv_service->isNewLoan()) {
                $this->endSession();
            } else {
                PriceSession::findByWebSessionId(
                    $this->csv_service->importConfig()->getWebSessionId()
                )->update([
                    "in_used" => 0,
                ]);
            }
        } catch (\Exception $e) {
            $this->error("Unable to get job details. " . $e->getMessage());
        }
    }

    /**
     * Run all process
     *
     * @return void
     */
    private function runProcess()
    {
        $this->progress = new CsvImportProgress();
        if (
            $this->csv_service->isResourceValid("loan_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "loan_data"
            )
        ) {
            $this->processLoanData();
        }

        if ($this->csv_service->isResourceValid("borrower_data")) {
            $this->processBorrowerInfo();
        }

        if ($this->csv_service->isResourceValid("customer_data")) {
            $this->processCustomerData();
        }

        if (
            $this->csv_service->isResourceValid("subject_property_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "subject_property_data"
            )
        ) {
            $this->processSubjectProperty();
        }

        if ($this->csv_service->isResourceValid("fee_data")) {
            $this->processFeeData();
        }

        if (
            $this->csv_service->isResourceValid("adjustment_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "adjustment_data"
            )
        ) {
            $this->processLoan2Data();
        }
        if (
            $this->csv_service->isResourceValid("loan_hmda_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "loan_hmda_data"
            )
        ) {
            $this->processLoanHmdaData();
        }
        if (
            $this->csv_service->isResourceValid("loan_servicing_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "loan_servicing_data"
            )
        ) {
            $this->processLoanServicingData();
        }
        if (
            $this->csv_service->isResourceValid("loan_correspondent_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "loan_correspondent_data"
            )
        ) {
            $this->processLoanCorrespondentData();
        }
        if ($this->csv_service->isResourceValid("loan_license_data")) {
            $this->processLoanLicense();
        }
        if (
            $this->csv_service->isResourceValid("company_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "company_data"
            )
        ) {
            $this->processCompanyData();
        }
        if (
            $this->csv_service->isResourceValid("virtual_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "virtual_data"
            )
        ) {
            $this->processVirtualData();
        }
        if (
            $this->csv_service->isResourceValid("extra_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "extra_data"
            )
        ) {
            $this->processExtraData();
        }
        if (
            $this->csv_service->isResourceValid("date_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "date_data"
            )
        ) {
            $this->processDateData();
        }
        if ($this->csv_service->isResourceValid("investor_code_data")) {
            $this->processInvestorFeatureCode();
        }
        if (
            $this->csv_service->isResourceValid("loan_correspondent_fee_data")
        ) {
            $this->processLoanCorrespondentFee();
        }
        if (
            $this->csv_service->isResourceValid(
                "loan_correspondent_adjustment_data"
            )
        ) {
            $this->processLoanCorrespondentAdjustment();
        }

        if (
            $this->csv_service->isResourceValid("uldd_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "uldd_data"
            )
        ) {
            $this->processUlddData();
        }

        if (
            $this->csv_service->isResourceValid("fnma_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "fnma_data"
            )
        ) {
            $this->processFnmaData();
        }

        if (
            $this->csv_service->isResourceValid("trade_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "trade_data"
            )
        ) {
            $this->processTradeData();
        }

        if (
            $this->csv_service->isResourceValid("uldd_additional_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "uldd_additional_data"
            )
        ) {
            $this->processUlddAdditionalData();
        }
    }

    private function runTradeSetupProcess()
    {
        $this->progress = new CsvImportProgress();
        if (
            $this->csv_service->isResourceValid("trade_setup_data") &&
            !$this->progress->isDataSetAlreadyProcessed(
                $this->csv_import_id,
                $this->row_num,
                "trade_setup_data"
            )
        ) {
            $this->processTradeSetupData();
        }
    }

    private function getEmailAddress()
    {
        $borrowerEmailAddress = self::TEMPORARY_EMAIL_ADDRESS;
        if ($this->csv_service->isResourceValid("borrower_data")) {
            $borrowers = $this->csv_service->getResourceProperty(
                "borrower_data"
            );
            if (
                is_array($borrowers) &&
                isset($borrowers[0]) &&
                !empty($borrowers[0]["email_address"])
            ) {
                $borrowerEmailAddress = $borrowers[0]["email_address"];
            }
        }
        return $borrowerEmailAddress;
    }
    /**
     * Determine loan ID
     *
     * @param $record
     * @return integer
     */
    private function setLoanId($record)
    {
        // resets the Loan ID query string
        $this->csv_service->price_config->setLoanId(0);
        $this->process_string = new \Price\Misc\ProcessString(
            $this->csv_service->price_config
        );
        if ($this->csv_service->isNewLoan()) {
            $channel = isset($record["Channel"]) ? $record["Channel"] : "";
            // If Broker_ID exists use it as passedID
            $passed_id = 0;
            if (isset($record["Broker_ID"]) && $record["Broker_ID"] !== "") {
                if ($this->brokerHasSecurityLevel($record["Broker_ID"])) {
                    $passed_id = $record["Broker_ID"];
                }
            }
            if (empty($passedId) && isset($record["Loan_Officer_ID"])) {
                $passed_id = $record["Loan_Officer_ID"];
            }

            $pwd = uniqid();
            $updateJob = new CsvImportJob();
            $updateJob
                ->where("csv_import_id", $this->csv_import_id)
                ->where("row_num", $this->row_num - 1)
                ->update([
                    "email_tmp" => Crypt::encryptString($pwd),
                ]);
            // add transaction
            $loan = new \Price\Resources\Loan($this->csv_service->price_config);
            $loan_id = $loan
                ->setQueryParams([
                    "Channel" => $channel,
                    "PassedID" => $passed_id,
                    "EMailAddress" => $this->getEmailAddress(),
                    "Document" . implode("", self::PWD_KY) => $pwd,
                ])
                ->create();
            if (!$loan_id) {
                throw new \Exception("Unable to create loan");
            }

            $this->info("Loan created");
        } else {
            // update trasaction
            $loan_id = (int) $record["Loan_Number_ID"];
        }
        // check if loan_id is valid - numeric and not 0
        if (!is_numeric($loan_id) || !$loan_id) {
            $this->csv_service->logImport([
                "row_num" => $this->row_num,
                "col_name" => "Loan",
                "col_headers" => "N/A",
                "status" => self::IMPORT_STATUS_FAILED,
                "description" => $this->csv_service->isNewLoan()
                    ? "Unable to create loan"
                    : "Loan ID is empty",
                "processing_time" => 0,
            ]);
            throw new \Exception("Unable to create loan");
        } else {
            $this->info("Processing Loan # " . $loan_id);
            $job = new CsvImportJob();
            $job->where("csv_import_id", $this->csv_import_id)
                ->where("row_num", $this->row_num - 1)
                ->update([
                    "loan_id" => $loan_id,
                ]);
        }

        $this->csv_service->price_config->setLoanId($loan_id);
        // If $loan is set, log the response time from ADD_A_LOAN
        if (isset($loan)) {
            $this->csv_service->logImport([
                "row_num" => $this->row_num,
                "col_name" => "Loan",
                "col_headers" => "N/A",
                "status" => self::IMPORT_STATUS_SUCCESS,
                "description" => "Loan created successfully",
                "processing_time" => $loan->getResponseTime(),
            ]);
        }
        // reinitialize Loan Resource class with $loan_id
        $this->loan_resource = new \Price\Resources\Loan(
            $this->csv_service->price_config
        );
        $this->process_string = new \Price\Misc\ProcessString(
            $this->csv_service->price_config
        );

        return $this;
    }

    private function checkEmpty($data)
    {
        if (!empty($data)) {
            return false;
        }

        return true;
    }

    private function checkIfRowFailed()
    {
        return $this->csv_service
            ->csvImport()
            ->logs()
            ->HasUnSupportedError()
            ->whereRowNum($this->row_num)
            ->count();
    }

    /**
     * Process borrower information data
     *
     * @return void
     */
    private function processBorrowerInfo()
    {
        $this->borrower_resource = new \Price\Resources\Borrowers(
            $this->csv_service->price_config
        );
        $borrowers = $this->borrower_resource->get();
        $response_time = $this->borrower_resource->getResponseTime();
        $data = $this->csv_service->getResourceProperty("borrower_data");
        if (!$this->checkEmpty($borrowers)) {
            foreach ($data as $index => $borrower_data) {
                $orig_headers = $this->csv_service->getOriginalHeaders(
                    array_keys($borrower_data),
                    "Borrower",
                    $index
                );
                if (
                    isset($borrowers[$index]) &&
                    !empty($borrower_data) &&
                    !$this->progress->isDataSetAlreadyProcessed(
                        $this->csv_import_id,
                        $this->row_num,
                        "borrower_data_" . $index
                    )
                ) {
                    $try = 0;
                    do {
                        $update = $this->borrower_resource
                            ->setResourceId($borrowers[$index]->PersonID)
                            ->update($borrower_data);
                        if (isset($update->Successful)) {
                            if ($update->Successful) {
                                $this->progress->add(
                                    $this->csv_import_id,
                                    $this->row_num,
                                    "borrower_data_" . $index
                                );
                                $this->csv_service->logImport([
                                    "col_name" => "Borrower",
                                    "col_headers" => $orig_headers,
                                    "row_num" => $this->row_num,
                                    "status" => self::IMPORT_STATUS_SUCCESS,
                                    "description" => "Borrower Info Success",
                                    "processing_time" =>
                                        $response_time +
                                        $update->Stats->MethodTime,
                                ]);
                                break;
                            } else {
                                if (
                                    $this->isErrorForRetry($update->ErrorCode)
                                ) {
                                    throw new \Exception($update->ErrorMessage);
                                } elseif (
                                    !$this->retryRequest(
                                        $try,
                                        $update->ErrorCode
                                    )
                                ) {
                                    $this->csv_service->logImport([
                                        "col_name" => "Borrower",
                                        "col_headers" => $orig_headers,
                                        "row_num" => $this->row_num,
                                        "status" => self::IMPORT_STATUS_FAILED,
                                        "description" =>
                                            "Borrower #" .
                                            ($index + 1) .
                                            " - [" .
                                            $update->ErrorCode .
                                            "]" .
                                            $update->ErrorMessage,
                                        "processing_time" =>
                                            $response_time +
                                            $update->Stats->MethodTime,
                                    ]);
                                    break;
                                }
                            }
                        } else {
                            $this->error(
                                "[PRICE][Error] - Invalid Response Received - processBorrowerInfo Section 1"
                            );
                            if ($this->isErrorForRetry()) {
                                $this->forceRetry = true;
                            } else {
                                $this->csv_service->logImport([
                                    "row_num" => $this->row_num,
                                    "col_name" => "Borrower",
                                    "col_headers" => $orig_headers,
                                    "status" => self::IMPORT_STATUS_FAILED,
                                    "description" =>
                                        "Borrower - Invalid Response Received",
                                    "processing_time" => 0,
                                ]);
                            }
                            break;
                        }
                        $try++;
                    } while ($try < 3);
                } else {
                    if (
                        !$this->progress->isDataSetAlreadyProcessed(
                            $this->csv_import_id,
                            $this->row_num,
                            "borrower_data_" . $index
                        )
                    ) {
                        // add new pair borrower
                        $new_borrowers = $this->borrower_resource->create();
                        $new_response_time =
                            $response_time +
                            $this->borrower_resource->getResponseTime();
                        if (is_array($new_borrowers)) {
                            for ($i = 0; $i < count($new_borrowers); $i++) {
                                // append new PersonIDs to the current set of $borrowers
                                $borrowers[$index + $i] = (object) [
                                    "PersonID" => $new_borrowers[$i],
                                ];
                            }
                            $try = 0;
                            do {
                                $update = $this->borrower_resource
                                    ->setResourceId(
                                        $borrowers[$index]->PersonID
                                    )
                                    ->update($borrower_data);
                                if (isset($update->Successful)) {
                                    if ($update->Successful) {
                                        $this->progress->add(
                                            $this->csv_import_id,
                                            $this->row_num,
                                            "borrower_data_" . $index
                                        );
                                        $this->csv_service->logImport([
                                            "row_num" => $this->row_num,
                                            "col_name" => "Borrower",
                                            "col_headers" => $orig_headers,
                                            "status" =>
                                                self::IMPORT_STATUS_SUCCESS,
                                            "description" =>
                                                "Borrower Info Success",
                                            "processing_time" =>
                                                $new_response_time +
                                                $update->Stats->MethodTime,
                                        ]);
                                        break;
                                    } else {
                                        if (
                                            $this->isErrorForRetry(
                                                $update->ErrorCode
                                            )
                                        ) {
                                            throw new \Exception(
                                                $update->ErrorMessage
                                            );
                                        } elseif (
                                            !$this->retryRequest(
                                                $try,
                                                $update->ErrorCode
                                            )
                                        ) {
                                            $this->csv_service->logImport([
                                                "row_num" => $this->row_num,
                                                "col_name" => "Borrower",
                                                "col_headers" => $orig_headers,
                                                "status" =>
                                                    self::IMPORT_STATUS_FAILED,
                                                "description" =>
                                                    "Borrower #" .
                                                    ($index + 1) .
                                                    " - [" .
                                                    $update->ErrorCode .
                                                    "]" .
                                                    $update->ErrorMessage,
                                                "processing_time" =>
                                                    $new_response_time +
                                                    $update->Stats->MethodTime,
                                            ]);
                                            break;
                                        }
                                    }
                                } else {
                                    $this->error(
                                        "[PRICE][Error] - Invalid Response Received - processBorrowerInfo Section 2"
                                    );
                                    if ($this->isErrorForRetry()) {
                                        $this->forceRetry = true;
                                    } else {
                                        $this->csv_service->logImport([
                                            "row_num" => $this->row_num,
                                            "col_name" => "Borrower",
                                            "col_headers" => $orig_headers,
                                            "status" =>
                                                self::IMPORT_STATUS_FAILED,
                                            "description" =>
                                                "Borrower - Invalid Response Received",
                                            "processing_time" => 0,
                                        ]);
                                    }
                                    break;
                                }
                                $try++;
                            } while ($try < 3);
                        } else {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "Borrower",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_NOTICE,
                                "description" =>
                                    "Unable to update data for Borrower #" .
                                    ($index + 1),
                                "processing_time" => $new_response_time,
                            ]);
                        }
                    }
                }
            }
        }
    }

    /**
     * Process customer data
     *
     * @return void
     */
    private function processCustomerData()
    {
        $customer_resource = new \Price\Resources\Customers(
            $this->csv_service->price_config
        );
        $customers = $customer_resource->get();
        $response_time = $customer_resource->getResponseTime();
        $data = $this->csv_service->getResourceProperty("customer_data");

        if (!$this->checkEmpty($customers)) {
            foreach ($data as $index => $customer_data) {
                $orig_headers = $this->csv_service->getOriginalHeaders(
                    array_keys($customer_data),
                    "Customer",
                    $index
                );
                if (isset($customer_data["BankruptcyIndicator"])) {
                    $customer_data["Declare_B"] =
                        $customer_data["BankruptcyIndicator"];
                }

                if (isset($customer_data["CitizenshipResidencyType"])) {
                    if (!$this->csv_service->isNewLoan()) {
                        $customer_data["Declare_J"] = "N";
                        $customer_data["Declare_K"] = "N";
                        $customer_data["NonPermanentResidentAlien"] = "false";
                        $customer_data["NonResidentAlien"] = "false";
                    }
                    if (
                        $customer_data["CitizenshipResidencyType"] ==
                        "U.S. Citizen"
                    ) {
                        $customer_data["Declare_J"] = "Y";
                    }
                    if (
                        $customer_data["CitizenshipResidencyType"] ==
                        "Permanent Resident Alien"
                    ) {
                        $customer_data["Declare_K"] = "Y";
                    }
                    if (
                        $customer_data["CitizenshipResidencyType"] ==
                        "Non-Permanent Resident Alien"
                    ) {
                        $customer_data["NonPermanentResidentAlien"] = "true";
                    }
                    if (
                        $customer_data["CitizenshipResidencyType"] ==
                        "Non-Resident Alien"
                    ) {
                        $customer_data["NonResidentAlien"] = "true";
                    }
                    // unset the header
                    unset($customer_data["CitizenshipResidencyType"]);
                }

                if (
                    isset($customer_data["LoanForeclosureOrJudgmentIndicator"])
                ) {
                    $customer_data[
                        "PriorPropertyForeclosureCompletedIndicator"
                    ] = $customer_data["Declare_A"] =
                        $customer_data["LoanForeclosureOrJudgmentIndicator"] ==
                        "Y"
                            ? "1"
                            : "0";

                    // unset the header
                    unset($customer_data["LoanForeclosureOrJudgmentIndicator"]);
                }

                if (isset($customers[$index])) {
                    if (
                        !$this->progress->isDataSetAlreadyProcessed(
                            $this->csv_import_id,
                            $this->row_num,
                            "customer_data_" . $index
                        )
                    ) {
                        $try = 0;
                        do {
                            $update = $customer_resource
                                ->setResourceId($customers[$index]->CustomerID)
                                ->update($customer_data);
                            if (isset($update->Successful)) {
                                if ($update->Successful) {
                                    $this->progress->add(
                                        $this->csv_import_id,
                                        $this->row_num,
                                        "customer_data_" . $index
                                    );
                                    $this->csv_service->logImport([
                                        "row_num" => $this->row_num,
                                        "col_name" => "Borrower",
                                        "col_headers" => $orig_headers,
                                        "status" => self::IMPORT_STATUS_SUCCESS,
                                        "description" =>
                                            "Borrower Info Success",
                                        "processing_time" =>
                                            $response_time +
                                            $update->Stats->MethodTime,
                                    ]);
                                    break;
                                } else {
                                    if (
                                        $this->isErrorForRetry(
                                            $update->ErrorCode
                                        )
                                    ) {
                                        throw new \Exception(
                                            $update->ErrorMessage
                                        );
                                    } elseif (
                                        !$this->retryRequest(
                                            $try,
                                            $update->ErrorCode
                                        )
                                    ) {
                                        $this->csv_service->logImport([
                                            "row_num" => $this->row_num,
                                            "col_name" => "Borrower",
                                            "col_headers" => $orig_headers,
                                            "status" =>
                                                self::IMPORT_STATUS_FAILED,
                                            "description" =>
                                                "Borrower #" .
                                                ($index + 1) .
                                                " - [" .
                                                $update->ErrorCode .
                                                "]" .
                                                $update->ErrorMessage,
                                            "processing_time" =>
                                                $response_time +
                                                $update->Stats->MethodTime,
                                        ]);
                                        break;
                                    }
                                }
                            } else {
                                $this->error(
                                    "[PRICE][Error] - Invalid Response Received - processCustomerData"
                                );
                                if ($this->isErrorForRetry()) {
                                    $this->forceRetry = true;
                                } else {
                                    $this->csv_service->logImport([
                                        "row_num" => $this->row_num,
                                        "col_name" => "Borrower",
                                        "col_headers" => $orig_headers,
                                        "status" => self::IMPORT_STATUS_FAILED,
                                        "description" =>
                                            "Borrower - Invalid Response Received",
                                        "processing_time" => 0,
                                    ]);
                                }
                                break;
                            }
                            $try++;
                        } while ($try < 3);
                    }
                } else {
                    $this->csv_service->logImport([
                        "row_num" => $this->row_num,
                        "col_name" => "Borrower",
                        "col_headers" => $orig_headers,
                        "status" => self::IMPORT_STATUS_NOTICE,
                        "description" =>
                            "Unable to update data for Borrower #" .
                            ($index + 1),
                        "processing_time" => $response_time,
                    ]);
                }
            }
        }
    }

    /**
     * Process subject property
     *
     * @return void
     */
    private function processSubjectProperty()
    {
        $subj_property_id = $this->process_string->handle([
            "DataLanguage" => "ihFind(,Loan,Property_id)",
        ]);
        $subj_property_customer_id = $this->process_string->handle([
            "DataLanguage" => "IhFind(,Loan,Customer_ID)",
        ]);
        $process_string_time = $this->process_string->getResponseTime();
        if (is_numeric($subj_property_id)) {
            $data = $this->csv_service->getResourceProperty(
                "subject_property_data"
            );
            if (isset($data[0])) {
                $mailing_address_headers = [
                    "Mailing_Address",
                    "Mailing_City",
                    "Mailing_State",
                    "Mailing_Zip",
                ];
                $orig_headers = $this->csv_service->getOriginalHeaders(
                    array_keys($data[0]),
                    "Property"
                );
                // remove mailing address from subject property address headers
                $arr_headers = explode(",", $orig_headers);
                $orig_headers = implode(
                    ",",
                    array_diff($arr_headers, $mailing_address_headers)
                );
                $property_resource = new \Price\Resources\Properties(
                    $this->csv_service->price_config
                );
                $response_time =
                    $process_string_time +
                    $property_resource->getResponseTime();
                if (isset($data[0]["Appraised_Value"])) {
                    $data[0]["Appraised_Value"] = number_format(
                        $data[0]["Appraised_Value"],
                        2
                    );
                }
                $data[0]["customer_id"] = $subj_property_customer_id;
                $try = 0;
                do {
                    $update = $property_resource
                        ->setResourceId($subj_property_id)
                        ->update($data[0]);
                    if (isset($update->Successful)) {
                        if ($update->Successful) {
                            $this->progress->add(
                                $this->csv_import_id,
                                $this->row_num,
                                "subject_property_data"
                            );
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "SubjectProperty",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_SUCCESS,
                                "description" =>
                                    "Subject Property Data Success",
                                "processing_time" =>
                                    $response_time + $update->Stats->MethodTime,
                            ]);
                            break;
                        } else {
                            if ($this->isErrorForRetry($update->ErrorCode)) {
                                throw new \Exception($update->ErrorMessage);
                            } elseif (
                                !$this->retryRequest($try, $update->ErrorCode)
                            ) {
                                $this->csv_service->logImport([
                                    "row_num" => $this->row_num,
                                    "col_name" => "SubjectProperty",
                                    "col_headers" => $orig_headers,
                                    "status" => self::IMPORT_STATUS_FAILED,
                                    "description" =>
                                        "Subject Property - [" .
                                        $update->ErrorCode .
                                        "]" .
                                        $update->ErrorMessage,
                                    "processing_time" =>
                                        $response_time +
                                        $update->Stats->MethodTime,
                                ]);
                                break;
                            }
                        }
                    } else {
                        $this->error(
                            "[PRICE][Error] - Invalid Response Received - processSubjectProperty"
                        );
                        if ($this->isErrorForRetry()) {
                            $this->forceRetry = true;
                        } else {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "SubjectProperty",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Subject Property - Invalid Response Received",
                                "processing_time" => 0,
                            ]);
                        }
                        break;
                    }
                    $try++;
                } while ($try < 3);
                $this->processMailingAddress($data);
            }
        }
    }

    /**
     * Process Mailing Address
     *
     * @param array $data
     * @return void
     */
    private function processMailingAddress(array $data)
    {
        // update mailing address if provided
        if (isset($data[0]["Mailing_Address"])) {
            $property_resource = new \Price\Resources\Properties(
                $this->csv_service->price_config
            );
            $mailing_address_id = $this->process_string->handle([
                "DataLanguage" =>
                    "ihFind(ihInfo(GetBorrowerNumber,1),Customer,Mailing_Address_Id)",
            ]);
            $process_string_time = $this->process_string->getResponseTime();
            if (is_numeric($mailing_address_id)) {
                $update = $property_resource
                    ->setResourceId($mailing_address_id)
                    ->update([
                        "customer_id" => $data[0]["customer_id"],
                        "Address" => $data[0]["Mailing_Address"],
                        "City" => isset($data[0]["Mailing_City"])
                            ? $data[0]["Mailing_City"]
                            : "",
                        "State" => isset($data[0]["Mailing_State"])
                            ? $data[0]["Mailing_State"]
                            : "",
                        "Zip" => isset($data[0]["Mailing_Zip"])
                            ? $data[0]["Mailing_Zip"]
                            : "",
                    ]);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->progress->add(
                            $this->csv_import_id,
                            $this->row_num,
                            "mailling_address"
                        );
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "MailingAddress",
                            "col_headers" =>
                                "Mailing_Address,Mailing_City,Mailing_State,Mailing_Zip",
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" => "Mailing Address Data Success",
                            "processing_time" =>
                                $process_string_time +
                                $update->Stats->MethodTime,
                        ]);
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "MailingAddress",
                            "col_headers" =>
                                "Mailing_Address,Mailing_City,Mailing_State,Mailing_Zip",
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "Mailing Address - [" .
                                $update->ErrorCode .
                                "]" .
                                $update->ErrorMessage,
                            "processing_time" =>
                                $process_string_time +
                                $update->Stats->MethodTime,
                        ]);
                    }
                } else {
                    $this->error(
                        "[PRICE][Error] Error running processMailingAddress"
                    );
                }
            }
        }
    }

    private function brokerHasSecurityLevel($brokerId)
    {
        $this->info("Checking Broker Security Level ${brokerId}");
        $contactId = filter_var($brokerId, FILTER_VALIDATE_INT);
        if (!empty($contactId)) {
            $result = $this->process_string->handle([
                "DataLanguage" => "ihInformation(Contact,${contactId},Contact_Security_Level)",
            ]);
            if (!empty($result)) {
                $this->info("Broker has security level");
                return true;
            }
        }
        $this->info("Broker does not have security level");
        return false;
    }

    /**
     * Process Loan Data
     *
     * @return void
     */
    private function processLoanData()
    {
        $data = $this->csv_service->getResourceProperty("loan_data");
        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "Loan"
            );

            // set related field if filled
            if (isset($data[0]["MortgageInsuranceZeroDueAtClosing"])) {
                $data[0]["MortgageInsuranceRefund"] =
                    $data[0]["MortgageInsuranceZeroDueAtClosing"] == "Yes"
                        ? "Y"
                        : "N";
            }

            /*
                FloodLifeOfLoanIndicator ,FloodNFIPMapIndicator, FloodPartialIndicator now working with TRUE/FALSE
            */

            if (isset($data[0]["FloodLifeOfLoanIndicator"])) {
                $data[0]["FloodLifeOfLoanIndicator"] =
                    strtolower($data[0]["FloodLifeOfLoanIndicator"]) == "true"
                        ? "1"
                        : "0";
            }

            if (isset($data[0]["FloodNFIPMapIndicator"])) {
                $data[0]["FloodNFIPMapIndicator"] =
                    strtolower($data[0]["FloodNFIPMapIndicator"]) == "true"
                        ? "Y"
                        : "N";
            }

            if (isset($data[0]["FloodPartialIndicator"])) {
                $data[0]["FloodPartialIndicator"] =
                    strtolower($data[0]["FloodPartialIndicator"]) == "true"
                        ? "Y"
                        : "N";
            }

            if (isset($data[0]["Commit_Program_Id"])) {
                $data[0]["Program_ID"] = $data[0]["Commit_Program_Id"];
                unset($data[0]["Commit_Program_Id"]);
            }

            if (isset($data[0]["Miscellaneous_Purchase_Advice_Fee_3"])) {
                $data[0]["Other_Transaction_Costs"] =
                    $data[0]["Miscellaneous_Purchase_Advice_Fee_3"];
                unset($data[0]["Miscellaneous_Purchase_Advice_Fee_3"]);
            }

            $try = 0;
            do {
                $update = $this->loan_resource->update($data[0]);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->progress->add(
                            $this->csv_import_id,
                            $this->row_num,
                            "loan_data"
                        );
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "LoanData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" => "Loan Data Success",
                            "processing_time" => $update->Stats->MethodTime,
                        ]);
                        break;
                    } else {
                        if ($this->isErrorForRetry($update->ErrorCode)) {
                            throw new \Exception($update->ErrorMessage);
                        } elseif (
                            !$this->retryRequest($try, $update->ErrorCode)
                        ) {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "LoanData",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Loan Data - [" .
                                    $update->ErrorCode .
                                    "]" .
                                    $update->ErrorMessage,
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        }
                    }
                } else {
                    if ($this->isErrorForRetry()) {
                        $this->error(
                            "[JOB][DOT] - Invalid Response Received - processLoanData"
                        );
                        $this->forceRetry = true;
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "LoanData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "Loan Data - Invalid Response Received",
                            "processing_time" => 0,
                        ]);
                    }
                    break;
                }
                $try++;
            } while ($try < 3);
        }
    }

    /**
     * Process Loan2 Data
     *
     * @return void
     */
    private function processLoan2Data()
    {
        $data = $this->csv_service->getResourceProperty("adjustment_data");
        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "Loan2"
            );
            $price_adjusments = [];
            if (isset($data[0]["wildcards"]) && count($data[0]["wildcards"])) {
                $price_adjusments["LockConfirmData"] = [];
                $price_adjusments["PriceAdjustmentsConfData"] = [];
                foreach ($data[0]["wildcards"] as $index => $adjustment) {
                    $ctr = $index + 1;
                    $price_adjusments["LockConfirmData"] = array_merge(
                        $price_adjusments["LockConfirmData"],
                        [
                            [
                                "SectionData" =>
                                    "CommitPriceAdjustments " . $ctr,
                                "SectionField" => "Name",
                                "SectionValue" => isset(
                                    $adjustment["Price_Adjustment_Description"]
                                )
                                    ? $adjustment[
                                        "Price_Adjustment_Description"
                                    ]
                                    : "",
                            ],
                            [
                                "SectionData" =>
                                    "CommitPriceAdjustments " . $ctr,
                                "SectionField" => "Amount",
                                "SectionValue" => isset(
                                    $adjustment["Price_Adjustment_Value"]
                                )
                                    ? $adjustment["Price_Adjustment_Value"]
                                    : "0",
                            ],
                            [
                                "SectionData" =>
                                    "CommitPriceAdjustments " . $ctr,
                                "SectionField" => "Enabled",
                                "SectionValue" => isset(
                                    $adjustment["Price_Adjustment"]
                                )
                                    ? $adjustment["Price_Adjustment"]
                                    : "0",
                            ],
                        ]
                    );

                    $price_adjusments["PriceAdjustmentsConfData"] = array_merge(
                        $price_adjusments["PriceAdjustmentsConfData"],
                        [
                            [
                                "SectionData" =>
                                    "PurchaseAdjustmentsData " . $ctr,
                                "SectionField" => "ID",
                                "SectionValue" => strval($ctr),
                            ],
                            [
                                "SectionData" =>
                                    "PurchaseAdjustmentsData " . $ctr,
                                "SectionField" => "Name",
                                "SectionValue" => isset(
                                    $adjustment["PriceAdjustment_Description"]
                                )
                                    ? $adjustment["PriceAdjustment_Description"]
                                    : "",
                            ],
                            [
                                "SectionData" =>
                                    "PurchaseAdjustmentsData " . $ctr,
                                "SectionField" => "DollarAmount",
                                "SectionValue" => isset(
                                    $adjustment["PriceAdjustment_Dollar"]
                                )
                                    ? $adjustment["PriceAdjustment_Dollar"]
                                    : "0",
                            ],
                            [
                                "SectionData" =>
                                    "PurchaseAdjustmentsData " . $ctr,
                                "SectionField" => "Amount",
                                "SectionValue" => isset(
                                    $adjustment["PriceAdjustment_Percent"]
                                )
                                    ? $adjustment["PriceAdjustment_Percent"]
                                    : "0",
                            ],
                        ]
                    );
                }
            }
            unset($data[0]["wildcards"]);
            $data[0] = array_merge($data[0], $price_adjusments);
            // due to stripping of prefix, other field name that includes the
            // same as the prefix, we need rename those keys to its correct key
            if (isset($data[0]["Base_Price"])) {
                $data[0]["Commit_Base_Price"] = $data[0]["Base_Price"];
                unset($data[0]["Base_Price"]);
            }

            $adjustment_resource = new \Price\Resources\Adjustment(
                $this->csv_service->price_config
            );
            $try = 0;
            do {
                $update = $adjustment_resource->update($data[0]);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->progress->add(
                            $this->csv_import_id,
                            $this->row_num,
                            "adjustment_data"
                        );
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "Loan2Data",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" => "Loan2 Data Success",
                            "processing_time" => $update->Stats->MethodTime,
                        ]);
                        break;
                    } else {
                        if ($this->isErrorForRetry($update->ErrorCode)) {
                            throw new \Exception($update->ErrorMessage);
                        } elseif (
                            !$this->retryRequest($try, $update->ErrorCode)
                        ) {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "Loan2Data",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Loan2 Data - [" .
                                    $update->ErrorCode .
                                    "]" .
                                    $update->ErrorMessage,
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        }
                    }
                } else {
                    $this->error(
                        "[PRICE][Error] - Invalid Response Received - processLoa2nData"
                    );
                    if ($this->isErrorForRetry()) {
                        $this->forceRetry = true;
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "Loan2Data",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "Loan2 Data - Invalid Response Received",
                            "processing_time" => 0,
                        ]);
                    }
                }
                $try++;
            } while ($try < 3);
        }
    }

    /**
     * Process Loan Servicing Data
     *
     * @return void
     */
    private function processLoanServicingData()
    {
        $data = $this->csv_service->getResourceProperty("loan_servicing_data");
        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "LoanServicing"
            );
            $loan_servicing = new \Price\Resources\LoanServicing(
                $this->csv_service->price_config
            );
            $try = 0;
            do {
                $update = $loan_servicing->update($data[0]);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->progress->add(
                            $this->csv_import_id,
                            $this->row_num,
                            "loan_servicing_data"
                        );
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "LoanServicingData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" => "Loan Servicing Data Success",
                            "processing_time" => $update->Stats->MethodTime,
                        ]);
                        break;
                    } else {
                        if ($this->isErrorForRetry($update->ErrorCode)) {
                            throw new \Exception($update->ErrorMessage);
                        } elseif (
                            !$this->retryRequest($try, $update->ErrorCode)
                        ) {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "LoanServicingData",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Loan Servicing Data - [" .
                                    $update->ErrorCode .
                                    "]" .
                                    $update->ErrorMessage,
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        }
                    }
                } else {
                    $this->error(
                        "[PRICE][Error] - Invalid Response Received - processLoanServicingData"
                    );
                    if ($this->isErrorForRetry()) {
                        $this->forceRetry = true;
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "LoanServicingData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "Loan Servicing Data - Invalid Response Received",
                            "processing_time" => 0,
                        ]);
                    }
                }
                $try++;
            } while ($try < 3);
        }
    }

    /**
     * Process Loan HMDA Data
     *
     * @return void
     */
    private function processLoanHmdaData()
    {
        $data = $this->csv_service->getResourceProperty("loan_hmda_data");
        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "LoanHMDA"
            );
            $loan_hmda = new \Price\Resources\LoanHmda(
                $this->csv_service->price_config
            );
            $try = 0;
            do {
                $update = $loan_hmda->update($data[0]);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->progress->add(
                            $this->csv_import_id,
                            $this->row_num,
                            "loan_hmda_data"
                        );
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "LoanHMDAData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" => "Loan HMDA Data Success",
                            "processing_time" => $update->Stats->MethodTime,
                        ]);
                        break;
                    } else {
                        if ($this->isErrorForRetry($update->ErrorCode)) {
                            throw new \Exception($update->ErrorMessage);
                        } elseif (
                            !$this->retryRequest($try, $update->ErrorCode)
                        ) {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "LoanHMDAData",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Loan HMDA Data - [" .
                                    $update->ErrorCode .
                                    "]" .
                                    $update->ErrorMessage,
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        }
                    }
                } else {
                    $this->error(
                        "[PRICE][Error] - Invalid Response Received - processLoanHmdaData"
                    );
                    if ($this->isErrorForRetry()) {
                        $this->forceRetry = true;
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "LoanHMDAData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "Loan HMDA Data - Invalid Response Received",
                            "processing_time" => 0,
                        ]);
                    }
                }
                $try++;
            } while ($try < 3);
        }
    }

    /**
     * Process Loan Correspondent Data
     *
     * @return void
     */
    private function processLoanCorrespondentData()
    {
        $data = $this->csv_service->getResourceProperty(
            "loan_correspondent_data"
        );
        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "LoanCorrespondent"
            );
            $loan_correspondent = new \Price\Resources\LoanCorrespondent(
                $this->csv_service->price_config
            );

            $try = 0;
            do {
                $update = $loan_correspondent->update($data[0]);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->progress->add(
                            $this->csv_import_id,
                            $this->row_num,
                            "loan_correspondent_data"
                        );
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "LoanCorrespondentData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" => "Loan correspondent Data Success",
                            "processing_time" => $update->Stats->MethodTime,
                        ]);
                        break;
                    } else {
                        if ($this->isErrorForRetry($update->ErrorCode)) {
                            throw new \Exception($update->ErrorMessage);
                        } elseif (
                            !$this->retryRequest($try, $update->ErrorCode)
                        ) {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "LoanCorrespondentData",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Loan correspondent Data - [" .
                                    $update->ErrorCode .
                                    "]" .
                                    $update->ErrorMessage,
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        }
                    }
                } else {
                    $this->error(
                        "[PRICE][Error] - Invalid Response Received - processLoanCorrespondentData"
                    );
                    if ($this->isErrorForRetry()) {
                        $this->forceRetry = true;
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "LoanCorrespondentData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "Loan correspondent Data - Invalid Response Received",
                            "processing_time" => 0,
                        ]);
                    }
                    break;
                }
                $try++;
            } while ($try < 3);
        }
    }

    /**
     * Process VirtualData
     *
     * @return void
     */
    private function processVirtualData()
    {
        $data = $this->csv_service->getResourceProperty("virtual_data");
        if (count($data)) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "VirtualData"
            );
            $virtual_data_resource = new \Price\Resources\VirtualData(
                $this->csv_service->price_config
            );
            $try = 0;
            do {
                $update = $virtual_data_resource->update($data[0]);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->progress->add(
                            $this->csv_import_id,
                            $this->row_num,
                            "virtual_data"
                        );
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "VirtualData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" => "Virtual Data Success",
                            "processing_time" => $update->Stats->MethodTime,
                        ]);
                        break;
                    } else {
                        if ($this->isErrorForRetry($update->ErrorCode)) {
                            throw new \Exception($update->ErrorMessage);
                        } elseif (
                            !$this->retryRequest($try, $update->ErrorCode)
                        ) {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "VirtualData",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Virtual Data - [" .
                                    $update->ErrorCode .
                                    "]" .
                                    $update->ErrorMessage,
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        }
                    }
                } else {
                    $this->error(
                        "[PRICE][Error] - Invalid Response Received - processVirtualData"
                    );
                    if ($this->isErrorForRetry()) {
                        $this->forceRetry = true;
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "VirtualData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "Virtual Data - Invalid Response Received",
                            "processing_time" => 0,
                        ]);
                    }
                    break;
                }
                $try++;
            } while ($try < 3);
        }
    }

    /**
     * Process ExtraData
     *
     * @return void
     */
    private function processExtraData()
    {
        $data = $this->csv_service->getResourceProperty("extra_data");
        if (count($data)) {
            $extra_data_resource = new \Price\Resources\ExtraData(
                $this->csv_service->price_config
            );
            foreach ($data[0] as $key => $value) {
                // Remove space and special characters
                $extra_data_name = preg_replace("/[^A-Za-z0-9\-]/", "", $key);
                $try = 0;
                do {
                    $update = $extra_data_resource->update([
                        "ExtraDataName" => $extra_data_name,
                        "ExtraDataValue" => $value,
                        "RowNumberID" => "",
                    ]);
                    if (isset($update->Successful)) {
                        if ($update->Successful) {
                            $this->progress->add(
                                $this->csv_import_id,
                                $this->row_num,
                                "extra_data"
                            );
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "ExtraData",
                                "col_headers" => "ExtraData_" . $key,
                                "status" => self::IMPORT_STATUS_SUCCESS,
                                "description" => "Extra Data Success",
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        } else {
                            if ($this->isErrorForRetry($update->ErrorCode)) {
                                throw new \Exception($update->ErrorMessage);
                            } elseif (
                                !$this->retryRequest($try, $update->ErrorCode)
                            ) {
                                $this->csv_service->logImport([
                                    "row_num" => $this->row_num,
                                    "col_name" => "ExtraData",
                                    "col_headers" => "ExtraData_" . $key,
                                    "status" => self::IMPORT_STATUS_FAILED,
                                    "description" =>
                                        "ExtraData_" .
                                        $key .
                                        " - [" .
                                        $update->ErrorCode .
                                        "]" .
                                        $update->ErrorMessage,
                                    "processing_time" =>
                                        $update->Stats->MethodTime,
                                ]);
                                break;
                            }
                        }
                    } else {
                        $this->error(
                            "[PRICE][Error] - Invalid Response Received - processExtraData"
                        );
                        if ($this->isErrorForRetry()) {
                            $this->forceRetry = true;
                        } else {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "ExtraData",
                                "col_headers" => "ExtraData_" . $key,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Extra Data - Invalid Response Received",
                                "processing_time" => 0,
                            ]);
                        }
                        break;
                    }
                    $try++;
                } while ($try < 3);
            }
        }
    }

    /**
     * Process Date Data
     *
     * @return void
     */
    private function processDateData()
    {
        $data = $this->csv_service->getResourceProperty("date_data");
        if (count($data)) {
            $add_or_update_date = new \Price\Misc\AddOrUpdateDate(
                $this->csv_service->price_config
            );
            foreach ($data[0] as $key => $value) {
                $try = 0;
                $date_name = $key;
                do {
                    $update = $add_or_update_date->handle([
                        "DateName" => $date_name,
                        "DateValue" => $value,
                    ]);
                    if (isset($update->Successful)) {
                        if ($update->Successful) {
                            $this->progress->add(
                                $this->csv_import_id,
                                $this->row_num,
                                "date_data"
                            );
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "DateData",
                                "col_headers" => "Date_" . $key,
                                "status" => self::IMPORT_STATUS_SUCCESS,
                                "description" => "Date Data Success",
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        } else {
                            if ($this->isErrorForRetry($update->ErrorCode)) {
                                throw new \Exception($update->ErrorMessage);
                            } elseif (
                                !$this->retryRequest($try, $update->ErrorCode)
                            ) {
                                $this->csv_service->logImport([
                                    "row_num" => $this->row_num,
                                    "col_name" => "DateData",
                                    "col_headers" => $date_name,
                                    "status" => self::IMPORT_STATUS_FAILED,
                                    "description" =>
                                        $date_name .
                                        " - [" .
                                        $update->ErrorCode .
                                        "]" .
                                        $update->ErrorMessage,
                                    "processing_time" =>
                                        $update->Stats->MethodTime,
                                ]);
                                break;
                            }
                        }
                    } else {
                        $this->error(
                            "[PRICE][Error] - Invalid Response Received - processDateData"
                        );
                        if ($this->isErrorForRetry()) {
                            $this->forceRetry = true;
                        } else {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "DateData",
                                "col_headers" => $date_name,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Date Data - Invalid Response Received",
                                "processing_time" => 0,
                            ]);
                        }
                        break;
                    }
                    $try++;
                } while ($try < 3);
            }
        }
    }

    /**
     * Process Fee Data
     *
     * @return void
     */
    private function processFeeData()
    {
        try {
            $data = $this->csv_service->getResourceProperty("fee_data");
            if (count($data)) {
                $fees = new \Price\Resources\Fees(
                    $this->csv_service->price_config
                );
                $loan_fees = new \Price\Resources\LoanFees(
                    $this->csv_service->price_config
                );
                $available_fees = collect($fees->get());
                $fee_proc_time = $fees->getResponseTime();
                if (!$available_fees->count()) {
                    $this->error("[DOT][Fees] Unable to get fees.");
                    $this->csv_service->logImport([
                        "row_num" => $this->row_num,
                        "col_name" => "FeeData",
                        "col_headers" => "ALL",
                        "status" => self::IMPORT_STATUS_FAILED,
                        "description" => "Unable to get fees",
                        "processing_time" => $fee_proc_time,
                    ]);
                } else {
                    $fee_data = [];
                    // collect all fee field
                    foreach ($data[0] as $key => $value) {
                        $col_header = explode("_", $key);
                        list(, $fee_id) = $col_header;
                        // store original headers for logs purposes
                        $fee_data[$fee_id]["headers"][] = $key;
                        // exploded header that > 3 contains most of PRICE fields
                        if (count($col_header) > 3) {
                            // Parse the PRICE field
                            $field = str_replace(
                                " ",
                                "_",
                                ucwords(end($col_header))
                            );
                            $fee_data[$fee_id][$field] = $value;
                        } else {
                            // fee amount
                            $fee_data[$fee_id]["Total"] = $value;
                        }
                    }

                    // excute fee datas to PRICE
                    $existing_loan_fees = $loan_fees->get();
                    $process_time =
                        $fee_proc_time + $loan_fees->getResponseTime();
                    foreach ($fee_data as $key => $attributes) {
                        if (
                            !$this->progress->isDataSetAlreadyProcessed(
                                $this->csv_import_id,
                                $this->row_num,
                                "fee_data_" . $key
                            )
                        ) {
                            $headers = implode(",", $attributes["headers"]);
                            // check if fee exists
                            $fee = $available_fees
                                ->where("FeeID", $key)
                                ->first();
                            if (!is_null($fee)) {
                                // check if Fee already exists in the loan
                                $current_loan_fee = $existing_loan_fees
                                    ->where("Fee_ID", $fee->FeeID)
                                    ->first();
                                if (!is_null($current_loan_fee)) {
                                    // Loan Fee exists
                                    $loan_fee_id =
                                        $current_loan_fee["Loan_Fee_ID"];
                                } else {
                                    // Loan Fee not exists
                                    $loan_fee_id = $loan_fees
                                        ->setQueryParams([
                                            "FeeID" => $fee->FeeID,
                                        ])
                                        ->create();
                                }
                                $try = 0;
                                do {
                                    $update = $loan_fees
                                        ->setResourceId($loan_fee_id)
                                        ->update($attributes);
                                    if (isset($update->Successful)) {
                                        if ($update->Successful) {
                                            $this->progress->add(
                                                $this->csv_import_id,
                                                $this->row_num,
                                                "fee_data_" . $key
                                            );
                                            $this->csv_service->logImport([
                                                "row_num" => $this->row_num,
                                                "col_name" => "FeeData",
                                                "col_headers" => $headers,
                                                "status" =>
                                                    self::IMPORT_STATUS_SUCCESS,
                                                "description" =>
                                                    "Fee data success",
                                                "processing_time" =>
                                                    $update->Stats->MethodTime,
                                            ]);
                                            break;
                                        } else {
                                            if (
                                                $this->isErrorForRetry(
                                                    $update->ErrorCode
                                                )
                                            ) {
                                                throw new \Exception(
                                                    $update->ErrorMessage
                                                );
                                            } elseif (
                                                !$this->retryRequest(
                                                    $try,
                                                    $update->ErrorCode
                                                )
                                            ) {
                                                $this->error(
                                                    "[DOT][Fees] Unable to add fee."
                                                );
                                                $this->csv_service->logImport([
                                                    "row_num" => $this->row_num,
                                                    "col_name" => "FeeData",
                                                    "col_headers" => $headers,
                                                    "status" =>
                                                        self::IMPORT_STATUS_FAILED,
                                                    "description" =>
                                                        "Unable to add fee.",
                                                    "processing_time" =>
                                                        $update->Stats
                                                            ->MethodTime,
                                                ]);
                                                break;
                                            }
                                        }
                                    } else {
                                        $this->error(
                                            "[PRICE][Error] - Invalid Response Received - processFeeData"
                                        );
                                        if ($this->isErrorForRetry()) {
                                            $this->forceRetry = true;
                                        } else {
                                            $this->csv_service->logImport([
                                                "row_num" => $this->row_num,
                                                "col_name" => "FeeData",
                                                "col_headers" => $headers,
                                                "status" =>
                                                    self::IMPORT_STATUS_FAILED,
                                                "description" =>
                                                    "FeeData - Invalid Response Received",
                                                "processing_time" => 0,
                                            ]);
                                        }
                                        break;
                                    }
                                    $try++;
                                } while ($try < 3);
                            } else {
                                $this->csv_service->logImport([
                                    "row_num" => $this->row_num,
                                    "col_name" => "FeeData",
                                    "col_headers" => $headers,
                                    "status" => self::IMPORT_STATUS_FAILED,
                                    "description" => "Unable to find fee",
                                    "processing_time" => $process_time,
                                ]);
                            }
                        }
                    }
                }
            }
        } catch (\Exception $e) {
            $this->error("FEE " . $e->getMessage());
        }
    }

    private function processCompanyData()
    {
        $data = $this->csv_service->getResourceProperty("company_data");
        if (count($data)) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "Company"
            );
            $company_id = $this->process_string->handle([
                "DataLanguage" =>
                    "ihFind(,Loan,ihFind(,Loan,Broker_Company_ID))",
            ]);
            $process_string_time = $this->process_string->getResponseTime();
            if (is_numeric($company_id)) {
                $company_resource = new \Price\Resources\Companies(
                    $this->csv_service->price_config
                );
                $try = 0;
                do {
                    $update = $company_resource
                        ->setResourceId($company_id)
                        ->update($data[0]);
                    if (isset($update->Successful)) {
                        if ($update->Successful) {
                            $this->progress->add(
                                $this->csv_import_id,
                                $this->row_num,
                                "company_data"
                            );
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "CompanyData",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_SUCCESS,
                                "description" => "Company Data Success",
                                "processing_time" =>
                                    $process_string_time +
                                    $update->Stats->MethodTime,
                            ]);
                            break;
                        } else {
                            if ($this->isErrorForRetry($update->ErrorCode)) {
                                throw new \Exception($update->ErrorMessage);
                            } elseif (
                                !$this->retryRequest($try, $update->ErrorCode)
                            ) {
                                $this->csv_service->logImport([
                                    "row_num" => $this->row_num,
                                    "col_name" => "CompanyData",
                                    "col_headers" => $orig_headers,
                                    "status" => self::IMPORT_STATUS_FAILED,
                                    "description" =>
                                        "Company Data - [" .
                                        $update->ErrorCode .
                                        "]" .
                                        $update->ErrorMessage,
                                    "processing_time" =>
                                        $process_string_time +
                                        $update->Stats->MethodTime,
                                ]);
                                break;
                            }
                        }
                    } else {
                        $this->error(
                            "[PRICE][Error] - Invalid Response Received - processCompanyData"
                        );
                        if ($this->isErrorForRetry()) {
                            $this->forceRetry = true;
                        } else {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "CompanyData",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Company Data - Invalid Response Received",
                                "processing_time" => 0,
                            ]);
                        }
                        break;
                    }
                    $try++;
                } while ($try < 3);
            }
        }
    }

    private function processLoanLicense()
    {
        $data = $this->csv_service->getResourceProperty("loan_license_data");
        if (count($data)) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "LoanLicense"
            );
            $loan_license = new \Price\Resources\LoanLicense(
                $this->csv_service->price_config
            );
            $attributes = [];
            $license_prefixes = [
                "Loan_Lender_" => $loan_license::DATA_FROM_LENDER,
                "LoanOfficer" => $loan_license::DATA_FROM_LOAN_OFFICER,
                "Broker" => $loan_license::DATA_FROM_BROKER_COMPANY,
                "BrokerPerson" => $loan_license::DATA_FROM_BROKER_PERSON,
            ];
            foreach ($data[0] as $key => $value) {
                // get the prefix
                $prefix = substr($key, 0, strpos($key, "License"));
                // check if prefix is valid
                if (isset($license_prefixes[$prefix])) {
                    $lists = isset($attributes[$prefix])
                        ? $attributes[$prefix]
                        : ["DataFrom" => $license_prefixes[$prefix]];
                    // replace the prefix to empty string to set the right field
                    // and remove _ to match PRICE field name
                    $attributes[$prefix] = array_merge($lists, [
                        str_replace(
                            "_",
                            "",
                            str_replace($prefix, "", $key)
                        ) => $value,
                    ]);
                }
            }
            // Execute the update for Loan License data
            foreach ($attributes as $prefix => $data) {
                if (
                    !$this->progress->isDataSetAlreadyProcessed(
                        $this->csv_import_id,
                        $this->row_num,
                        "loan_license_data_" . $prefix
                    )
                ) {
                    $try = 0;
                    do {
                        $update = $loan_license->update($data);
                        if (isset($update->Successful)) {
                            if ($update->Successful) {
                                $this->progress->add(
                                    $this->csv_import_id,
                                    $this->row_num,
                                    "loan_license_data_" . $prefix
                                );
                                $this->csv_service->logImport([
                                    "row_num" => $this->row_num,
                                    "col_name" => "LoanLicenseData",
                                    "col_headers" => $orig_headers,
                                    "status" => self::IMPORT_STATUS_SUCCESS,
                                    "description" =>
                                        "Loan License " . $prefix . " Success",
                                    "processing_time" =>
                                        $update->Stats->MethodTime,
                                ]);
                                break;
                            } else {
                                if (
                                    $this->isErrorForRetry($update->ErrorCode)
                                ) {
                                    throw new \Exception($update->ErrorMessage);
                                } elseif (
                                    !$this->retryRequest(
                                        $try,
                                        $update->ErrorCode
                                    )
                                ) {
                                    $this->csv_service->logImport([
                                        "row_num" => $this->row_num,
                                        "col_name" => "LoanLicenseData",
                                        "col_headers" => $orig_headers,
                                        "status" => self::IMPORT_STATUS_FAILED,
                                        "description" =>
                                            "Loan License " .
                                            $prefix .
                                            " - [" .
                                            $update->ErrorCode .
                                            "]" .
                                            $update->ErrorMessage,
                                        "processing_time" =>
                                            $update->Stats->MethodTime,
                                    ]);
                                    break;
                                }
                            }
                        } else {
                            $this->error(
                                "[PRICE][Error] - Invalid Response Received - processLoanLicense"
                            );
                            if ($this->isErrorForRetry()) {
                                $this->forceRetry = true;
                            } else {
                                $this->csv_service->logImport([
                                    "row_num" => $this->row_num,
                                    "col_name" => "LoanLicenseData",
                                    "col_headers" => $orig_headers,
                                    "status" => self::IMPORT_STATUS_FAILED,
                                    "description" =>
                                        "Loan License - Invalid Response Received",
                                    "processing_time" => 0,
                                ]);
                            }
                            break;
                        }
                        $try++;
                    } while ($try < 3);
                }
            }
        }
    }

    private function processInvestorFeatureCode()
    {
        try {
            $data = $this->csv_service->getResourceProperty(
                "investor_code_data"
            );
            if (count($data)) {
                $orig_headers = $this->csv_service->getOriginalHeaders(
                    array_keys($data[0]),
                    "InvestorFeatureCode"
                );
                $investor_code = new \Price\Resources\InvestorFeatureCode(
                    $this->csv_service->price_config
                );
                $investors = ["FNMA" => "0", "FHLMC" => "1"];
                foreach ($data[0] as $key => $value) {
                    if (
                        !$this->progress->isDataSetAlreadyProcessed(
                            $this->csv_import_id,
                            $this->row_num,
                            "investor_code_data_" . $key
                        )
                    ) {
                        list($investor) = explode("_", $key);
                        if (isset($investors[$investor])) {
                            $try = 0;
                            do {
                                $update = $investor_code->update([
                                    "FEATURECODETYPE" => $investors[$investor],
                                    "FEATURECODES" => $value,
                                ]);
                                if (isset($update->Successful)) {
                                    if ($update->Successful) {
                                        $this->progress->add(
                                            $this->csv_import_id,
                                            $this->row_num,
                                            "investor_code_data_" . $key
                                        );
                                        $this->csv_service->logImport([
                                            "row_num" => $this->row_num,
                                            "col_name" => "InvestorFeatureCode",
                                            "col_headers" => $orig_headers,
                                            "status" =>
                                                self::IMPORT_STATUS_SUCCESS,
                                            "description" =>
                                                "Investor Feature Code Success",
                                            "processing_time" =>
                                                $update->Stats->MethodTime,
                                        ]);
                                        break;
                                    } else {
                                        if (
                                            $this->isErrorForRetry(
                                                $update->ErrorCode
                                            )
                                        ) {
                                            throw new \Exception(
                                                $update->ErrorMessage
                                            );
                                        } elseif (
                                            !$this->retryRequest(
                                                $try,
                                                $update->ErrorCode
                                            )
                                        ) {
                                            if (is_object($update)) {
                                                $error_desc =
                                                    "Investor Feature Code - [" .
                                                    $update->ErrorCode .
                                                    "]" .
                                                    $update->ErrorMessage;
                                                $proc_time =
                                                    $update->Stats->MethodTime;
                                            } else {
                                                $uuid = substr(
                                                    Uuid::generate()->string,
                                                    0,
                                                    8
                                                );
                                                $error_desc =
                                                    "Server error. Reference ID:" .
                                                    $uuid;
                                                $this->error(
                                                    "[DOT][ERROR][" .
                                                        $uuid .
                                                        "] " .
                                                        $update
                                                );
                                                $proc_time = 0;
                                            }
                                            $this->csv_service->logImport([
                                                "row_num" => $this->row_num,
                                                "col_name" =>
                                                    "InvestorFeatureCode",
                                                "col_headers" => $orig_headers,
                                                "status" =>
                                                    self::IMPORT_STATUS_FAILED,
                                                "description" => $error_desc,
                                                "processing_time" => $proc_time,
                                            ]);
                                            break;
                                        }
                                    }
                                } else {
                                    $this->error(
                                        "[PRICE][Error] - Invalid Response Received - processInvestorFeatureCode"
                                    );
                                    if ($this->isErrorForRetry()) {
                                        $this->forceRetry = true;
                                    } else {
                                        $this->csv_service->logImport([
                                            "row_num" => $this->row_num,
                                            "col_name" => "InvestorFeatureCode",
                                            "col_headers" => $orig_headers,
                                            "status" =>
                                                self::IMPORT_STATUS_FAILED,
                                            "description" =>
                                                "Investor - Invalid Response Received",
                                            "processing_time" => 0,
                                        ]);
                                    }
                                    break;
                                }
                                $try++;
                            } while ($try < 3);
                        } else {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "InvestorFeatureCode",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Investor " .
                                    $investor .
                                    " is not supported.",
                            ]);
                        }
                    }
                }
            }
        } catch (\Exception $e) {
            $uuid = substr(Uuid::generate()->string, 0, 8);
            $this->error("[DOT][ERROR][" . $uuid . "] " . $e->getMessage());
            $this->csv_service->logImport([
                "row_num" => $this->row_num,
                "col_name" => "InvestorFeatureCode",
                "col_headers" => $orig_headers,
                "status" => self::IMPORT_STATUS_FAILED,
                "description" => "Server error. Reference ID: " . $uuid,
            ]);
        }
    }

    /**
     * Loan Correspondent Fee
     *
     * @return void
     */
    private function processLoanCorrespondentFee()
    {
        try {
            $data = $this->csv_service->getResourceProperty(
                "loan_correspondent_fee_data"
            );
            if (isset($data[0]["wildcards"]) && count($data[0]["wildcards"])) {
                foreach ($data[0]["wildcards"] as $index => $fee) {
                    if (
                        !$this->progress->isDataSetAlreadyProcessed(
                            $this->csv_import_id,
                            $this->row_num,
                            "loan_correspondent_fee_data_" . $index
                        )
                    ) {
                        $attributes = [];
                        $orig_headers = $this->csv_service->getOriginalHeaders(
                            array_keys($data[0]["wildcards"][$index]),
                            "LoanCorrespondentFee",
                            $index
                        );
                        $attributes = [
                            "LoanFeeID" => isset($fee["LoanFeeID"])
                                ? $fee["LoanFeeID"]
                                : 0,
                        ];
                        if (isset($fee["FeeID"])) {
                            $attributes["FeeID"] = (float) $fee["FeeID"];
                        }
                        if (isset($fee["FeeDescription"])) {
                            $attributes["Name"] = $fee["FeeDescription"];
                        }
                        if (isset($fee["FeeAmount"])) {
                            $attributes["Value"] = (float) $fee["FeeAmount"];
                        }

                        $correspondent_fees = new \Price\Resources\LoanCorrespondentFee(
                            $this->csv_service->price_config
                        );
                        $try = 0;
                        do {
                            $update = $correspondent_fees->update($attributes);
                            if (isset($update->Successful)) {
                                if ($update->Successful) {
                                    $this->progress->add(
                                        $this->csv_import_id,
                                        $this->row_num,
                                        "loan_correspondent_fee_data_" . $index
                                    );
                                    $this->csv_service->logImport([
                                        "row_num" => $this->row_num,
                                        "col_name" =>
                                            "LoanCorrespondentFeeData",
                                        "col_headers" => $orig_headers,
                                        "status" => self::IMPORT_STATUS_SUCCESS,
                                        "description" =>
                                            "Correspondent Fee Data Success",
                                        "processing_time" =>
                                            $update->Stats->MethodTime,
                                    ]);
                                    break;
                                } else {
                                    if (
                                        $this->isErrorForRetry(
                                            $update->ErrorCode
                                        )
                                    ) {
                                        throw new \Exception(
                                            $update->ErrorMessage
                                        );
                                    } elseif (
                                        !$this->retryRequest(
                                            $try,
                                            $update->ErrorCode
                                        )
                                    ) {
                                        $this->csv_service->logImport([
                                            "row_num" => $this->row_num,
                                            "col_name" =>
                                                "LoanCorrespondentFeeData",
                                            "col_headers" => $orig_headers,
                                            "status" =>
                                                self::IMPORT_STATUS_FAILED,
                                            "description" =>
                                                "Correspondent Fee Data - [" .
                                                $update->ErrorCode .
                                                "]" .
                                                $update->ErrorMessage,
                                            "processing_time" =>
                                                $update->Stats->MethodTime,
                                        ]);
                                        break;
                                    }
                                }
                            } else {
                                $this->error(
                                    "[PRICE][Error] - Invalid Response Received - processLoanCorrespondentFee"
                                );
                                if ($this->isErrorForRetry()) {
                                    $this->forceRetry = true;
                                } else {
                                    $this->csv_service->logImport([
                                        "row_num" => $this->row_num,
                                        "col_name" =>
                                            "LoanCorrespondentFeeData",
                                        "col_headers" => $orig_headers,
                                        "status" => self::IMPORT_STATUS_FAILED,
                                        "description" =>
                                            "Correspondent Fee Data - Invalid Response Received",
                                        "processing_time" => 0,
                                    ]);
                                }
                                break;
                            }
                            $try++;
                        } while ($try < 3);
                    }
                }
            }
        } catch (\Exception $e) {
            $ref = time();
            $this->csv_service->logImport([
                "row_num" => $this->row_num,
                "col_name" => "LoanCorrespondentFeeData",
                "col_headers" => $orig_headers,
                "status" => self::IMPORT_STATUS_FAILED,
                "description" =>
                    "[Ref #" . $ref . "] System error. Please contact support.",
                "processing_time" => 0,
            ]);
            $this->error(
                "[DOT][CORRESPONDENT FEE] Ref #$ref Message: " .
                    $e->getMessage()
            );
        }
    }

    /**
     * Loan Correspondent Adjustment
     *
     * @return void
     */
    private function processLoanCorrespondentAdjustment()
    {
        try {
            $data = $this->csv_service->getResourceProperty(
                "loan_correspondent_adjustment_data"
            );
            if (isset($data[0]["wildcards"]) && count($data[0]["wildcards"])) {
                foreach ($data[0]["wildcards"] as $index => $adjustment) {
                    if (
                        !$this->progress->isDataSetAlreadyProcessed(
                            $this->csv_import_id,
                            $this->row_num,
                            "loan_correspondent_adjustment_data_" . $index
                        )
                    ) {
                        $attributes = [];
                        $orig_headers = $this->csv_service->getOriginalHeaders(
                            array_keys($data[0]["wildcards"][$index]),
                            "LoanCorrespondentAdjustment",
                            $index
                        );
                        $attributes = [
                            "AdjustmentID" => isset($adjustment["AdjustmentID"])
                                ? $adjustment["AdjustmentID"]
                                : $index + 1,
                            "Enabled" => isset(
                                $adjustment["PriceAdjustmentEnabled"]
                            )
                                ? (bool) $adjustment["PriceAdjustmentEnabled"]
                                : true,
                        ];
                        if (isset($adjustment["PriceAdjustmentDescription"])) {
                            $attributes["AdjustmentName"] =
                                $adjustment["PriceAdjustmentDescription"];
                        }
                        if (isset($adjustment["PriceAdjustmentAmount"])) {
                            $attributes["Amount"] =
                                (float) $adjustment["PriceAdjustmentAmount"];
                        }

                        $correspondent_adj = new \Price\Resources\LoanCorrespondentAdjustment(
                            $this->csv_service->price_config
                        );
                        $try = 0;
                        do {
                            $update = $correspondent_adj->update($attributes);
                            if (isset($update->Successful)) {
                                if ($update->Successful) {
                                    $this->progress->add(
                                        $this->csv_import_id,
                                        $this->row_num,
                                        "loan_correspondent_adjustment_data_" .
                                            $index
                                    );
                                    $this->csv_service->logImport([
                                        "row_num" => $this->row_num,
                                        "col_name" =>
                                            "LoanCorrespondentAdjustmentData",
                                        "col_headers" => $orig_headers,
                                        "status" => self::IMPORT_STATUS_SUCCESS,
                                        "description" =>
                                            "Correspondent Adjustment Data Success",
                                        "processing_time" =>
                                            $update->Stats->MethodTime,
                                    ]);
                                    break;
                                } else {
                                    if (
                                        $this->isErrorForRetry(
                                            $update->ErrorCode
                                        )
                                    ) {
                                        throw new \Exception(
                                            $update->ErrorMessage
                                        );
                                    } elseif (
                                        !$this->retryRequest(
                                            $try,
                                            $update->ErrorCode
                                        )
                                    ) {
                                        $this->csv_service->logImport([
                                            "row_num" => $this->row_num,
                                            "col_name" =>
                                                "LoanCorrespondentAdjustmentData",
                                            "col_headers" => $orig_headers,
                                            "status" =>
                                                self::IMPORT_STATUS_FAILED,
                                            "description" =>
                                                "Correspondent Adjustment Data - [" .
                                                $update->ErrorCode .
                                                "]" .
                                                $update->ErrorMessage,
                                            "processing_time" =>
                                                $update->Stats->MethodTime,
                                        ]);
                                        break;
                                    }
                                }
                            } else {
                                $this->error(
                                    "[PRICE][Error] - Invalid Response Received - processLoanCorrespondentAdjustment"
                                );
                                if ($this->isErrorForRetry()) {
                                    $this->forceRetry = true;
                                } else {
                                    $this->csv_service->logImport([
                                        "row_num" => $this->row_num,
                                        "col_name" =>
                                            "LoanCorrespondentAdjustmentData",
                                        "col_headers" => $orig_headers,
                                        "status" => self::IMPORT_STATUS_FAILED,
                                        "description" =>
                                            "Correspondent Adjustment Data - Invalid Response Received",
                                        "processing_time" => 0,
                                    ]);
                                }
                                break;
                            }
                            $try++;
                        } while ($try < 3);
                    }
                }
            }
        } catch (\Exception $e) {
            $ref = time();
            $this->csv_service->logImport([
                "row_num" => $this->row_num,
                "col_name" => "LoanCorrespondentAdjustmentData",
                "col_headers" => $orig_headers,
                "status" => self::IMPORT_STATUS_FAILED,
                "description" =>
                    "[Ref #" . $ref . "] System error. Please contact support.",
                "processing_time" => 0,
            ]);
            $this->error(
                "[DOT][CORRESPONDENT ADJUSTMENT] Ref #$ref Message: " .
                    $e->getMessage()
            );
        }
    }

    private function processUlddData()
    {
        $data = $this->csv_service->getResourceProperty("uldd_data");
        if (count($data)) {
            $uldd = new \Price\Resources\UlddClosingData(
                $this->csv_service->price_config
            );

            $orig_headers = "";
            if (isset($data[0]["wildcards"]) && count($data[0]["wildcards"])) {
                foreach ($data[0]["wildcards"] as $index => $uldd_item) {
                    $orig_headers .=
                        "," .
                        $this->csv_service->getOriginalHeaders(
                            array_keys($data[0]["wildcards"][$index]),
                            "UlddClosingData",
                            $index
                        );
                }
                $orig_headers = substr($orig_headers, 1);

                $data = $data[0]["wildcards"];
                foreach ($data as &$datum) {
                    if (isset($datum["ContributionAmount"])) {
                        $datum["ContributionAmount"] =
                            (float) $datum["ContributionAmount"];
                    }
                    if (isset($datum["FreddieClosingID"])) {
                        $datum["FreddieClosingID"] =
                            (int) $datum["FreddieClosingID"];
                    }
                }
            }
            $try = 0;
            do {
                $update = $uldd->update($data);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->progress->add(
                            $this->csv_import_id,
                            $this->row_num,
                            "uldd_data"
                        );
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "UlddClosingData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" => "Uldd Closing Data Success",
                            "processing_time" => $update->Stats->MethodTime,
                        ]);
                        break;
                    } else {
                        if ($this->isErrorForRetry($update->ErrorCode)) {
                            throw new \Exception($update->ErrorMessage);
                        } elseif (
                            !$this->retryRequest($try, $update->ErrorCode)
                        ) {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "UlddClosingData",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Uldd Closing Data - [" .
                                    $update->ErrorCode .
                                    "]" .
                                    $update->ErrorMessage,
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        }
                    }
                } else {
                    $this->error(
                        "[PRICE][Error] - Invalid Response Received - processUlddData"
                    );
                    if ($this->isErrorForRetry()) {
                        $this->forceRetry = true;
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "UlddClosingData",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "Uldd Closing Data - Invalid Response Received",
                            "processing_time" => 0,
                        ]);
                    }
                    break;
                }
                $try++;
            } while ($try < 3);
        }
    }

    private function processUlddAdditionalData()
    {
        $this->borrower_resource = new \Price\Resources\Borrowers(
            $this->csv_service->price_config
        );
        $borrowers = $this->borrower_resource->get();
        $data = $this->csv_service->getResourceProperty("uldd_additional_data");
        if (count($data)) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "UlddAdditionalInfo"
            );

            $uldd = new \Price\Resources\UlddAdditionalInfo(
                $this->csv_service->price_config
            );

            $customer_uldd_information = [];
            foreach ($data as $index => &$datum) {
                if ($index > 0) {
                    $orig_headers .=
                        "," .
                        $this->csv_service->getOriginalHeaders(
                            array_keys($datum),
                            "UlddAdditionalInfo",
                            $index
                        );
                }
                if (!isset($datum["LoanNumberID"])) {
                    $datum[
                        "LoanNumberID"
                    ] = (string) $this->csv_service->price_config->getLoanId();
                }
                $customer_data = [];
                if (isset($datum["ULDDQualifyingIncomeAmount"])) {
                    $customer_data["ULDDQualifyingIncomeAmount"] =
                        (string) $datum["ULDDQualifyingIncomeAmount"];
                }
                if (isset($datum["ULDDSelfEmployed"])) {
                    $customer_data["ULDDSelfEmployed"] =
                        $datum["ULDDSelfEmployed"];
                }

                $customer_uldd_information[] = [
                    "CustomerID" => (int) $borrowers[$index]->PersonID,
                    "Fields" => $customer_data,
                ];
            }
            $data[0]["ULDDCustomerInformation"] = $customer_uldd_information;

            $try = 0;
            do {
                $update = $uldd->update($data);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->progress->add(
                            $this->csv_import_id,
                            $this->row_num,
                            "uldd_additional_data"
                        );
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "UlddAdditionalInfo",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" =>
                                "Uldd Additional Information Data Success",
                            "processing_time" => $update->Stats->MethodTime,
                        ]);
                        break;
                    } else {
                        if ($this->isErrorForRetry($update->ErrorCode)) {
                            throw new \Exception($update->ErrorMessage);
                        } elseif (
                            !$this->retryRequest($try, $update->ErrorCode)
                        ) {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "UlddAdditionalInfo",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Uldd Additional Information Data - [" .
                                    $update->ErrorCode .
                                    "]" .
                                    $update->ErrorMessage,
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        }
                    }
                } else {
                    $this->error(
                        "[PRICE][Error] - Invalid Response Received - processUlddAdditionalData"
                    );
                    if ($this->isErrorForRetry()) {
                        $this->forceRetry = true;
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "UlddAdditionalInfo",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "Uldd Additional Information Data - Invalid Response Received",
                            "processing_time" => 0,
                        ]);
                    }
                    break;
                }
                $try++;
            } while ($try < 3);
        }
    }

    private function processFnmaData()
    {
        $data = $this->csv_service->getResourceProperty("fnma_data");
        if (count($data)) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "FnmaSellingSystem"
            );
            $fnma = new \Price\Resources\FnmaSellingSystem(
                $this->csv_service->price_config
            );
            $try = 0;
            do {
                $update = $fnma->update($data[0]);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->progress->add(
                            $this->csv_import_id,
                            $this->row_num,
                            "fnma_data"
                        );
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "FnmaSellingSystem",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" => "FNMA Data Success",
                            "processing_time" => $update->Stats->MethodTime,
                        ]);
                        break;
                    } else {
                        if ($this->isErrorForRetry($update->ErrorCode)) {
                            throw new \Exception($update->ErrorMessage);
                        } elseif (
                            !$this->retryRequest($try, $update->ErrorCode)
                        ) {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "FnmaSellingSystem",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "FNMA Data - [" .
                                    $update->ErrorCode .
                                    "]" .
                                    $update->ErrorMessage,
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        }
                    }
                } else {
                    $this->error(
                        "[PRICE][Error] - Invalid Response Received - processFnmaData"
                    );
                    if ($this->isErrorForRetry()) {
                        $this->forceRetry = true;
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "FnmaSellingSystem",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "FNMA Data - Invalid Response Received",
                            "processing_time" => 0,
                        ]);
                    }
                    break;
                }
                $try++;
            } while ($try < 3);
        }
    }

    private function retryRequest($tryCount, $errorCode)
    {
        return $tryCount < self::ERROR_RETRY ||
            in_array($errorCode, [self::PRICE_ERR_INVALID_JSON_FORMAT_RETRY]);
    }
    private function isErrorForRetry($errorCode = "")
    {
        return $this->tries < CsvImportJob::MAX_TRIES - 1 &&
            (empty($errorCode) ||
                in_array($errorCode, [
                    self::PRICE_ERR_INVALID_SESSION,
                    self::PRICE_ERR_INVALID_NONCE,
                    self::PRICE_ERR_SESSION_EXPIRED,
                ]));
    }

    /**
     * Process Trade Data
     *
     * @return void
     */
    private function processTradeData()
    {
        $data = $this->csv_service->getResourceProperty("trade_data");
        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "Trade"
            );
            $trade = new \Price\Resources\Trade(
                $this->csv_service->price_config
            );
            $try = 0;
            do {
                $update = $trade->update($data[0]);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "Trade ID",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" => "Trade Data Success",
                            "processing_time" => $update->Stats->MethodTime,
                        ]);
                        break;
                    } else {
                        if ($this->isErrorForRetry($update->ErrorCode)) {
                            throw new \Exception($update->ErrorMessage);
                        } elseif (
                            !$this->retryRequest($try, $update->ErrorCode)
                        ) {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "Trade ID",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "Trade Data - [" .
                                    $update->ErrorCode .
                                    "]" .
                                    $update->ErrorMessage,
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        }
                    }
                } else {
                    $this->error(
                        "[PRICE][Error] - Invalid Response Received - processTradeData"
                    );
                    if ($this->isErrorForRetry()) {
                        $this->forceRetry = true;
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "Trade ID",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "Trade Data - Invalid Response Received",
                            "processing_time" => 0,
                        ]);
                    }
                    break;
                }
                $try++;
            } while ($try < 3);
        }
    }

    private function processTradeSetupData()
    {
        $dataTradeID = 0;
        $data = $this->csv_service->getResourceProperty("trade_setup_data");

        if (isset($data[0])) {
            $orig_headers = $this->csv_service->getOriginalHeaders(
                array_keys($data[0]),
                "TradeSetup"
            );
            $trade = new \Price\Resources\TradeSetup(
                $this->csv_service->price_config
            );
            $try = 0;
            $trade->setResourceId($dataTradeID);

            if (isset($data[0]["PoolBalloonIndicator"])) {
                $data[0]["PoolBalloonIndicator"] =
                    strtolower($data[0]["PoolBalloonIndicator"]) === "yes"
                        ? "true"
                        : "false";
            }
            if (isset($data[0]["PoolAssumability"])) {
                $data[0]["PoolAssumability"] =
                    strtolower($data[0]["PoolAssumability"]) === "yes"
                        ? "true"
                        : "false";
            }
            if (isset($data[0]["PoolInterestOnly"])) {
                $data[0]["PoolInterestOnly"] =
                    strtolower($data[0]["PoolInterestOnly"]) === "yes"
                        ? "true"
                        : "false";
            }

            do {
                $update = $trade->update($data[0]);
                if (isset($update->Successful)) {
                    if ($update->Successful) {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "TradeSetup",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_SUCCESS,
                            "description" => "Trade Setup Data Success",
                            "processing_time" => $update->Stats->MethodTime,
                            "loan_id" => $update->TradeID,
                        ]);
                        $job = new CsvImportJob();
                        $job->where("csv_import_id", $this->csv_import_id)
                            ->where("row_num", $this->row_num - 1)
                            ->update([
                                "loan_id" => $update->TradeID,
                            ]);
                        break;
                    } else {
                        if ($this->isErrorForRetry($update->ErrorCode)) {
                            throw new \Exception($update->ErrorMessage);
                        } elseif (
                            !$this->retryRequest($try, $update->ErrorCode)
                        ) {
                            $this->csv_service->logImport([
                                "row_num" => $this->row_num,
                                "col_name" => "TradeSetup",
                                "col_headers" => $orig_headers,
                                "status" => self::IMPORT_STATUS_FAILED,
                                "description" =>
                                    "TradeSetup Data - [" .
                                    $update->ErrorCode .
                                    "]" .
                                    $update->ErrorMessage,
                                "processing_time" => $update->Stats->MethodTime,
                            ]);
                            break;
                        }
                    }
                } else {
                    $this->error(
                        "[PRICE][Error] - Invalid Response Received - processTradeSetupData"
                    );
                    if ($this->isErrorForRetry()) {
                        $this->forceRetry = true;
                    } else {
                        $this->csv_service->logImport([
                            "row_num" => $this->row_num,
                            "col_name" => "TradeSetup",
                            "col_headers" => $orig_headers,
                            "status" => self::IMPORT_STATUS_FAILED,
                            "description" =>
                                "TradeSetup Data - Invalid Response Received",
                            "processing_time" => 0,
                        ]);
                    }
                    break;
                }
                $try++;
            } while ($try < 3);
        }
    }

    private function error($msg)
    {
        \Log::error($this->formatLogMsg($msg));
    }

    private function info($msg)
    {
        \Log::info($this->formatLogMsg($msg));
    }

    private function formatLogMsg($msg)
    {
        $rowNum = $this->row_num;
        return "[DOT][ImportCSV][" .
            $this->csv_service->price_config->getWebSessionId() .
            "][$this->csv_import_id][$rowNum] " .
            $msg;
    }
}
