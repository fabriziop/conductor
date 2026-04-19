/* .+
 * .context    : C++ implementation of a task scheduler with Python binding
 * .title      : Main C++ source file for the task scheduler implementation
 * .kind       : C++ source file
 * .author     : Fabrizio Pollastri <f.pollastri@inrim.it>
 * .site       : Revello - Italy
 * .creation   : 2026-04-10
 * .copyright  : (c) 2026 Fabrizio Pollastri <mxgbot@gmail.com>
 * .license    : GPL-3.0-or-later
 * .description
 * C++ implementation of a task scheduler with Python bindings using pybind11
 * and Boost.Asio.
 * .-
 */

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <boost/asio.hpp>

#include <atomic>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace py = pybind11;
namespace asio = boost::asio;
using steady_clock_t = std::chrono::steady_clock;

class Scheduler {
public:
    explicit Scheduler(std::uint64_t default_pool_workers = 1)
                : io_(1) {
        if (default_pool_workers == 0) {
            throw std::runtime_error("default_pool_workers must be >= 1");
        }
        create_pool_unlocked(default_pool_id(), static_cast<std::size_t>(default_pool_workers));
    }

    ~Scheduler() {
        stop();
        shutdown_pools();
    }

    void clear_tasks() {
        if (running_.load()) {
            throw std::runtime_error("cannot clear tasks while scheduler is running");
        }
        std::lock_guard<std::mutex> lock(mutex_);
        jobs_.clear();
        next_job_seq_ = 1;
        reset_global_stats_unlocked();
    }

    std::string add_task(
        py::object task,
        py::object args = py::none(),
        std::uint64_t period_us = 1000,
        py::object start = py::none(),
        std::int64_t offset_us = 0,
        py::object count = py::none(),
        py::object task_id = py::none(),
        py::object pool_id = py::none(),
        py::object overlap_policy = py::none()
    ) {
        if (running_.load()) {
            throw std::runtime_error("cannot add tasks while scheduler is running");
        }
        if (period_us == 0) {
            throw std::runtime_error("period_us must be > 0");
        }
        if (task.is_none()) {
            throw std::runtime_error("task must not be None");
        }

        auto job = std::make_shared<Job>(io_);
        job->period = std::chrono::microseconds(period_us);
        job->remaining = parse_count(count);
        job->next_deadline = resolve_start_deadline(start, offset_us);
        job->pool_id = parse_pool_id(pool_id);
        job->overlap_policy = parse_overlap_policy(overlap_policy);
        bind_task(*job, task, args);

        std::lock_guard<std::mutex> lock(mutex_);
        require_pool_unlocked(job->pool_id);
        job->id = task_id.is_none() ? next_default_job_id_unlocked() : task_id.cast<std::string>();
        std::string out = job->id;
        jobs_.push_back(job);
        return out;
    }

    void create_pool(py::object pool_id = py::none(), std::uint64_t workers = 1) {
        std::string name = parse_pool_id(pool_id);
        if (workers == 0) {
            throw std::runtime_error("workers must be >= 1");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        if (pools_.find(name) != pools_.end()) {
            throw std::runtime_error("pool already exists: " + name);
        }
        create_pool_unlocked(name, workers);
    }

    void remove_pool(py::object pool_id) {
        std::string name = parse_pool_id(pool_id);
        if (name == default_pool_id()) {
            throw std::runtime_error("cannot remove default pool");
        }

        std::shared_ptr<PoolState> removed;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (running_.load()) {
                throw std::runtime_error("cannot remove pools while scheduler is running");
            }
            for (const auto& job : jobs_) {
                if (job->pool_id == name) {
                    throw std::runtime_error("cannot remove pool assigned to existing tasks: " + name);
                }
            }

            auto it = pools_.find(name);
            if (it == pools_.end()) {
                throw std::runtime_error("unknown pool: " + name);
            }

            removed = it->second;
            pools_.erase(it);
        }

        removed->executor.join();
    }

    py::list list_pools() const {
        std::lock_guard<std::mutex> lock(mutex_);
        py::list out;
        for (const auto& kv : pools_) {
            py::dict item;
            item["id"] = kv.first;
            item["workers"] = py::int_(kv.second->worker_count);
            item["queued"] = py::int_(kv.second->queued);
            item["running"] = py::int_(kv.second->running);
            item["completed"] = py::int_(kv.second->completed);
            item["submitted"] = py::int_(kv.second->submitted);
            out.append(item);
        }
        return out;
    }

    py::object read_pool_stats(py::object pool_id = py::none()) const {
        std::lock_guard<std::mutex> lock(mutex_);

        auto to_dict = [](const std::string& id, const PoolState& pool) {
            py::dict out;
            out["id"] = id;
            out["workers"] = py::int_(pool.worker_count);
            out["queued"] = py::int_(pool.queued);
            out["running"] = py::int_(pool.running);
            out["completed"] = py::int_(pool.completed);
            out["submitted"] = py::int_(pool.submitted);
            return out;
        };

        if (pool_id.is_none()) {
            py::dict all;
            for (const auto& kv : pools_) {
                all[kv.first.c_str()] = to_dict(kv.first, *kv.second);
            }
            return std::move(all);
        }

        std::string id = parse_pool_id(pool_id);
        auto it = pools_.find(id);
        if (it == pools_.end()) {
            throw std::runtime_error("unknown pool: " + id);
        }
        return to_dict(id, *it->second);
    }

    void start_engine() {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true)) {
            throw std::runtime_error("scheduler already running");
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (jobs_.empty()) {
                running_.store(false);
                throw std::runtime_error("no tasks scheduled");
            }

            stop_requested_.store(false);
            reset_global_stats_unlocked();

            for (auto& job : jobs_) {
                job->active = true;
                job->finished_naturally = false;
                job->run_count = 0;
                job->late_count = 0;
                job->max_late_us = 0.0;
                job->mean_abs_error_us = 0.0;
                job->m2_abs_error = 0.0;
                job->skipped_count = 0;
                job->queue_wait_mean_us = 0.0;
                job->queue_wait_max_us = 0.0;
                job->queue_wait_m2 = 0.0;
                job->pending_finish = false;
                job->task_error.clear();
            }
        }

        work_guard_.reset();
        work_guard_.emplace(io_.get_executor());
        io_.restart();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& job : jobs_) {
                arm_job(job);
            }
        }

        worker_ = std::thread([this]() {
            try {
                io_.run();
            } catch (...) {
                // keep stop semantics simple
            }
            running_.store(false);
        });
    }

    void stop() {
        stop_requested_.store(true);

        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& job : jobs_) {
                job->timer.cancel();
                job->active = false;
            }

            if (total_in_flight_ == 0) {
                stop_io_unlocked();
            }
        }

        if (worker_.joinable()) {
            worker_.join();
        }

        io_.restart();
        work_guard_.reset();
        running_.store(false);
    }

    std::map<std::string, py::object> read_stats() const {
        std::lock_guard<std::mutex> lock(mutex_);

        std::map<std::string, py::object> out;
        out["running"] = py::bool_(running_.load());
        out["task_count"] = py::int_(static_cast<std::uint64_t>(jobs_.size()));
        out["total_runs"] = py::int_(total_runs_);
        out["late_runs"] = py::int_(late_runs_);
        out["max_late_us"] = py::float_(max_late_us_);
        out["mean_abs_error_us"] = py::float_(mean_abs_error_us_);

        double variance = 0.0;
        if (total_runs_ > 1) {
            variance = m2_abs_error_ / static_cast<double>(total_runs_ - 1);
        }
        out["stddev_abs_error_us"] = py::float_(std::sqrt(variance));
        return out;
    }

    py::list read_jobs() const {
        std::lock_guard<std::mutex> lock(mutex_);
        py::list out;

        for (const auto& job : jobs_) {
            py::dict d;
            d["id"] = job->id;
            d["kind"] = "python";
            d["name"] = job->display_name;
            d["pool_id"] = job->pool_id;
            d["overlap_policy"] = overlap_policy_name(job->overlap_policy);
            d["active"] = job->active;
            d["in_flight"] = py::int_(job->in_flight);
            d["suspended"] = py::bool_(job->suspended);
            d["resume_after_slots"] = suspension_state_to_object(*job);
            d["run_count"] = job->run_count;
            d["skipped_count"] = job->skipped_count;
            d["late_count"] = job->late_count;
            d["max_late_us"] = job->max_late_us;
            d["mean_abs_error_us"] = job->mean_abs_error_us;
            d["queue_wait_mean_us"] = job->queue_wait_mean_us;
            d["queue_wait_max_us"] = job->queue_wait_max_us;
            d["finished_naturally"] = job->finished_naturally;
            d["task_error"] = job->task_error;
            d["period_us"] = py::int_(job->period.count());
            if (job->remaining.has_value()) {
                d["remaining"] = *job->remaining;
            } else {
                d["remaining"] = py::str("forever");
            }
            out.append(d);
        }

        return out;
    }

    py::object suspend(py::object task_id = py::none(), std::uint64_t for_slots = 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto job = resolve_job_for_control_unlocked(task_id, "suspend");
        if (!job->active) {
            throw std::runtime_error("cannot suspend inactive task: " + job->id);
        }

        job->suspended = true;
        if (for_slots == 0) {
            job->suspend_countdown_slots.reset();
        } else {
            job->suspend_countdown_slots = for_slots;
        }

        return suspension_state_to_object(*job);
    }

    py::object resume(py::object task_id = py::none(), std::uint64_t after_slots = 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto job = resolve_job_for_control_unlocked(task_id, "resume");
        if (!job->active) {
            throw std::runtime_error("cannot resume inactive task: " + job->id);
        }

        if (!job->suspended) {
            if (after_slots == 0) {
                return py::int_(0);
            }
            throw std::runtime_error(
                "cannot delay resume for task that is not suspended: " + job->id
            );
        }

        if (after_slots == 0) {
            job->suspended = false;
            job->suspend_countdown_slots.reset();
        } else {
            job->suspended = true;
            job->suspend_countdown_slots = after_slots;
        }

        return suspension_state_to_object(*job);
    }

private:
    enum class OverlapPolicy { Serial, Overlap, Skip };

    struct PoolState {
        explicit PoolState(std::size_t workers)
            : worker_count(workers),
              executor(workers) {}

        std::size_t worker_count;
        asio::thread_pool executor;
        std::uint64_t queued{0};
        std::uint64_t running{0};
        std::uint64_t completed{0};
        std::uint64_t submitted{0};
    };

    struct Job : std::enable_shared_from_this<Job> {
        explicit Job(asio::io_context& io) : timer(io) {}

        std::string id;
        std::string display_name;
        std::string pool_id{default_pool_id()};
        OverlapPolicy overlap_policy{OverlapPolicy::Serial};

        py::object py_task{py::none()};
        py::tuple py_args{};

        asio::steady_timer timer;
        steady_clock_t::time_point next_deadline;
        std::chrono::microseconds period{1000};
        std::optional<std::uint64_t> remaining;
        bool active{true};
        std::uint64_t in_flight{0};
        std::uint64_t skipped_count{0};
        bool suspended{false};
        std::optional<std::uint64_t> suspend_countdown_slots;
        double queue_wait_mean_us{0.0};
        double queue_wait_max_us{0.0};
        double queue_wait_m2{0.0};
        bool pending_finish{false};

        std::uint64_t run_count{0};
        std::uint64_t late_count{0};
        double max_late_us{0.0};
        double mean_abs_error_us{0.0};
        double m2_abs_error{0.0};
        bool finished_naturally{false};
        std::string task_error;
    };

    static std::string normalize_name(std::string s) {
        for (char& c : s) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        return s;
    }

    static std::string trim(const std::string& s) {
        std::size_t begin = 0;
        while (begin < s.size() && std::isspace(static_cast<unsigned char>(s[begin]))) {
            ++begin;
        }
        std::size_t end = s.size();
        while (end > begin && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
            --end;
        }
        return s.substr(begin, end - begin);
    }

    static py::tuple normalize_args(const py::object& args_obj) {
        if (args_obj.is_none()) {
            return py::tuple();
        }
        if (py::isinstance<py::tuple>(args_obj)) {
            return args_obj.cast<py::tuple>();
        }
        if (py::isinstance<py::list>(args_obj)) {
            return py::tuple(args_obj);
        }
        return py::make_tuple(args_obj);
    }

    static const std::string& default_pool_id() {
        static const std::string id = "default";
        return id;
    }

    static std::string parse_pool_id(const py::object& pool_id_obj) {
        if (pool_id_obj.is_none()) {
            return default_pool_id();
        }
        if (!py::isinstance<py::str>(pool_id_obj)) {
            throw std::runtime_error("pool_id must be None or a string");
        }

        std::string pool_id = normalize_name(trim(pool_id_obj.cast<std::string>()));
        if (pool_id.empty()) {
            throw std::runtime_error("pool_id must not be empty");
        }
        return pool_id;
    }

    static const char* overlap_policy_name(OverlapPolicy policy) {
        switch (policy) {
        case OverlapPolicy::Serial:
            return "serial";
        case OverlapPolicy::Overlap:
            return "overlap";
        case OverlapPolicy::Skip:
            return "skip";
        }
        return "serial";
    }

    static OverlapPolicy parse_overlap_policy(const py::object& overlap_policy_obj) {
        if (overlap_policy_obj.is_none()) {
            return OverlapPolicy::Serial;
        }
        if (!py::isinstance<py::str>(overlap_policy_obj)) {
            throw std::runtime_error("overlap_policy must be None or a string");
        }

        std::string policy = normalize_name(trim(overlap_policy_obj.cast<std::string>()));
        if (policy == "serial") {
            return OverlapPolicy::Serial;
        }
        if (policy == "overlap") {
            return OverlapPolicy::Overlap;
        }
        if (policy == "skip") {
            return OverlapPolicy::Skip;
        }
        throw std::runtime_error("overlap_policy must be one of: serial, overlap, skip");
    }

    static std::optional<std::uint64_t> parse_count(const py::object& count_obj) {
        if (count_obj.is_none()) {
            return std::nullopt;
        }
        if (!py::isinstance<py::int_>(count_obj)) {
            throw std::runtime_error("count must be None or an integer >= 1");
        }
        long long n = count_obj.cast<long long>();
        if (n <= 0) {
            throw std::runtime_error("count must be >= 1");
        }
        return static_cast<std::uint64_t>(n);
    }

    static std::chrono::system_clock::time_point floor_to_second(
        std::chrono::system_clock::time_point tp
    ) {
        return std::chrono::time_point_cast<std::chrono::seconds>(tp);
    }

    static std::chrono::system_clock::time_point floor_to_minute(
        std::chrono::system_clock::time_point tp
    ) {
        auto sec_tp = std::chrono::time_point_cast<std::chrono::seconds>(tp);
        auto since_epoch = sec_tp.time_since_epoch();
        auto mins = std::chrono::duration_cast<std::chrono::minutes>(since_epoch);
        return std::chrono::system_clock::time_point(mins);
    }

    static std::chrono::system_clock::time_point floor_to_hour(
        std::chrono::system_clock::time_point tp
    ) {
        auto sec_tp = std::chrono::time_point_cast<std::chrono::seconds>(tp);
        auto since_epoch = sec_tp.time_since_epoch();
        auto hrs = std::chrono::duration_cast<std::chrono::hours>(since_epoch);
        return std::chrono::system_clock::time_point(hrs);
    }

    static std::chrono::system_clock::time_point parse_explicit_wallclock(
        const std::string& raw
    ) {
        static const std::regex re(
            R"(^\s*(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?\s*$)"
        );

        std::smatch m;
        if (!std::regex_match(raw, m, re)) {
            throw std::runtime_error("invalid explicit time format: " + raw);
        }

        std::tm tm{};
        tm.tm_year = std::stoi(m[1].str()) - 1900;
        tm.tm_mon  = std::stoi(m[2].str()) - 1;
        tm.tm_mday = std::stoi(m[3].str());
        tm.tm_hour = std::stoi(m[4].str());
        tm.tm_min  = std::stoi(m[5].str());
        tm.tm_sec  = std::stoi(m[6].str());
        tm.tm_isdst = -1;

        std::time_t tt = std::mktime(&tm);
        if (tt == static_cast<std::time_t>(-1)) {
            throw std::runtime_error("failed to convert explicit local time: " + raw);
        }

        auto tp = std::chrono::system_clock::from_time_t(tt);

        if (m[7].matched) {
            std::string frac = m[7].str();
            while (frac.size() < 9) frac.push_back('0');
            if (frac.size() > 9) frac.resize(9);
            tp += std::chrono::nanoseconds(std::stoll(frac));
        }

        return tp;
    }

    static steady_clock_t::time_point resolve_start_deadline(
        const py::object& start_obj,
        std::int64_t offset_us
    ) {
        std::chrono::system_clock::time_point system_tp;

        if (start_obj.is_none()) {
            system_tp = std::chrono::system_clock::now();
        } else if (py::isinstance<py::str>(start_obj)) {
            std::string raw = start_obj.cast<std::string>();
            std::string s = normalize_name(trim(raw));
            auto now = std::chrono::system_clock::now();

            if (s == "asap" || s == "now") {
                system_tp = now;
            } else if (s == "next_second") {
                system_tp = floor_to_second(now) + std::chrono::seconds(1);
            } else if (s == "next_minute") {
                system_tp = floor_to_minute(now) + std::chrono::minutes(1);
            } else if (s == "next_hour") {
                system_tp = floor_to_hour(now) + std::chrono::hours(1);
            } else {
                system_tp = parse_explicit_wallclock(raw);
            }
        } else if (py::isinstance<py::float_>(start_obj) || py::isinstance<py::int_>(start_obj)) {
            long double value = start_obj.cast<long double>();
            if (value < 1.0e12L) {
                auto ns = static_cast<long long>(value * 1.0e9L);
                system_tp = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ns));
            } else {
                auto ns = static_cast<long long>(value);
                system_tp = std::chrono::system_clock::time_point(std::chrono::nanoseconds(ns));
            }
        } else {
            throw std::runtime_error(
                "start must be None, number, or string "
                "('asap', 'now', 'next_second', 'next_minute', 'next_hour', "
                "'YYYY-MM-DD HH:MM:SS[.frac]')"
            );
        }

        system_tp += std::chrono::microseconds(offset_us);

        auto sys_now = std::chrono::system_clock::now();
        auto steady_now = steady_clock_t::now();
        return steady_now + (system_tp - sys_now);
    }

    void bind_task(Job& job, const py::object& task_obj, const py::object& args_obj) {
        py::tuple args = normalize_args(args_obj);

        if (PyCallable_Check(task_obj.ptr())) {
            job.display_name = "<python>";
            job.py_task = task_obj;
            job.py_args = args;
            return;
        }

        throw std::runtime_error("task must be a Python callable");
    }

    void update_global_stats_unlocked(double abs_error_us, bool late) {
        ++total_runs_;
        if (late) {
            ++late_runs_;
            if (abs_error_us > max_late_us_) {
                max_late_us_ = abs_error_us;
            }
        }

        double delta = abs_error_us - mean_abs_error_us_;
        mean_abs_error_us_ += delta / static_cast<double>(total_runs_);
        double delta2 = abs_error_us - mean_abs_error_us_;
        m2_abs_error_ += delta * delta2;
    }

    static void update_job_stats(Job& job, double error_us) {
        ++job.run_count;
        double abs_error_us = std::abs(error_us);

        if (error_us > 0.0) {
            ++job.late_count;
            if (error_us > job.max_late_us) {
                job.max_late_us = error_us;
            }
        }

        double delta = abs_error_us - job.mean_abs_error_us;
        job.mean_abs_error_us += delta / static_cast<double>(job.run_count);
        double delta2 = abs_error_us - job.mean_abs_error_us;
        job.m2_abs_error += delta * delta2;
    }

    static void update_job_queue_wait_stats(Job& job, double queue_wait_us) {
        if (queue_wait_us > job.queue_wait_max_us) {
            job.queue_wait_max_us = queue_wait_us;
        }

        // run_count has already been incremented for this invocation.
        if (job.run_count == 0) {
            return;
        }

        double delta = queue_wait_us - job.queue_wait_mean_us;
        job.queue_wait_mean_us += delta / static_cast<double>(job.run_count);
        double delta2 = queue_wait_us - job.queue_wait_mean_us;
        job.queue_wait_m2 += delta * delta2;
    }

    void reset_global_stats_unlocked() {
        total_runs_ = 0;
        late_runs_ = 0;
        max_late_us_ = 0.0;
        mean_abs_error_us_ = 0.0;
        m2_abs_error_ = 0.0;
        total_in_flight_ = 0;
    }

    std::string next_default_job_id_unlocked() {
        return "job_" + std::to_string(next_job_seq_++);
    }

    void create_pool_unlocked(const std::string& pool_id, std::size_t workers) {
        pools_[pool_id] = std::make_shared<PoolState>(workers);
    }

    std::shared_ptr<PoolState> require_pool_unlocked(const std::string& pool_id) const {
        auto it = pools_.find(pool_id);
        if (it == pools_.end()) {
            throw std::runtime_error("unknown pool: " + pool_id);
        }
        return it->second;
    }

    bool has_active_jobs_unlocked() const {
        for (const auto& job : jobs_) {
            if (job->active) {
                return true;
            }
        }
        return false;
    }

    bool should_stop_io_unlocked() const {
        if (stop_requested_.load()) {
            return total_in_flight_ == 0;
        }
        return !has_active_jobs_unlocked() && total_in_flight_ == 0;
    }

    void stop_io_unlocked() {
        if (work_guard_) {
            work_guard_->reset();
        }
        io_.stop();
    }

    void shutdown_pools() {
        std::vector<std::shared_ptr<PoolState>> pools;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& kv : pools_) {
                pools.push_back(kv.second);
            }
            pools_.clear();
        }

        for (auto& pool : pools) {
            pool->executor.join();
        }
    }

    void invoke_job(Job& job) {
        CurrentJobScope current_job_scope(job.shared_from_this());
        py::gil_scoped_acquire gil;
        job.py_task(*job.py_args);
    }

    static py::object suspension_state_to_object(const Job& job) {
        if (!job.suspended) {
            return py::int_(0);
        }
        if (!job.suspend_countdown_slots.has_value()) {
            return py::str("forever");
        }
        return py::int_(*job.suspend_countdown_slots);
    }

    std::shared_ptr<Job> resolve_job_for_control_unlocked(
        const py::object& task_id_obj,
        const char* action_name
    ) const {
        if (task_id_obj.is_none()) {
            auto current = tls_current_job_.lock();
            if (!current) {
                throw std::runtime_error(
                    std::string("task_id is required when ") + action_name
                    + " is called outside a task"
                );
            }
            return current;
        }

        if (!py::isinstance<py::str>(task_id_obj)) {
            throw std::runtime_error("task_id must be None or a string");
        }

        std::string task_id = task_id_obj.cast<std::string>();
        for (const auto& job : jobs_) {
            if (job->id == task_id) {
                return job;
            }
        }

        throw std::runtime_error("unknown task id: " + task_id);
    }

    void finish_job(Job& job) {
        job.active = false;
        job.finished_naturally = true;
        job.pending_finish = false;
    }

    void mark_job_schedule_complete_unlocked(Job& job) {
        job.active = false;
        if (job.in_flight == 0) {
            job.finished_naturally = true;
            job.pending_finish = false;
        } else {
            job.pending_finish = true;
        }
    }

    bool consume_remaining_run_unlocked(Job& job) {
        if (!job.remaining.has_value()) {
            return false;
        }
        if (*job.remaining > 0) {
            --(*job.remaining);
        }
        return *job.remaining == 0;
    }

    void advance_next_deadline_unlocked(Job& job, steady_clock_t::time_point anchor_time) {
        job.next_deadline += job.period;
        if (anchor_time > job.next_deadline + job.period * 10) {
            job.next_deadline = anchor_time + job.period;
        }
    }

    void on_job_complete(
        const std::shared_ptr<Job>& job,
        steady_clock_t::time_point fired_at,
        std::optional<std::string> error_message
    ) {
        bool rearm = false;
        bool stop_io = false;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (job->in_flight > 0) {
                --job->in_flight;
            }
            if (total_in_flight_ > 0) {
                --total_in_flight_;
            }

            if (error_message.has_value()) {
                job->task_error = *error_message;
                job->active = false;
                job->pending_finish = false;
                job->timer.cancel();
            } else if (
                job->overlap_policy == OverlapPolicy::Serial &&
                !stop_requested_.load() &&
                job->active
            ) {
                if (job->remaining.has_value()) {
                    if (consume_remaining_run_unlocked(*job)) {
                        finish_job(*job);
                    }
                }

                if (job->active) {
                    advance_next_deadline_unlocked(*job, fired_at);
                    rearm = true;
                }
            } else if (!error_message.has_value() && job->pending_finish && job->in_flight == 0) {
                job->finished_naturally = true;
                job->pending_finish = false;
            }

            stop_io = should_stop_io_unlocked();
        }

        if (rearm) {
            arm_job(job);
        } else if (stop_io) {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_io_unlocked();
        }
    }

    void arm_job(const std::shared_ptr<Job>& job) {
        if (!job->active || stop_requested_.load()) {
            return;
        }

        job->timer.expires_at(job->next_deadline);
        job->timer.async_wait([this, job](const boost::system::error_code& ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            if (ec) {
                bool stop_io = false;
                std::lock_guard<std::mutex> lock(mutex_);
                job->task_error = ec.message();
                job->active = false;
                stop_io = should_stop_io_unlocked();
                if (stop_io) {
                    stop_io_unlocked();
                }
                return;
            }

            auto fired_at = steady_clock_t::now();
            double error_us =
                std::chrono::duration<double, std::micro>(fired_at - job->next_deadline).count();

            std::shared_ptr<PoolState> pool;
            bool dispatch = false;
            bool rearm = false;
            bool stop_io = false;

            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (!job->active || stop_requested_.load()) {
                    return;
                }
                pool = require_pool_unlocked(job->pool_id);

                if (job->suspended) {
                    ++job->skipped_count;
                    if (job->suspend_countdown_slots.has_value()) {
                        if (*job->suspend_countdown_slots > 0) {
                            --(*job->suspend_countdown_slots);
                        }
                        if (*job->suspend_countdown_slots == 0) {
                            job->suspended = false;
                            job->suspend_countdown_slots.reset();
                        }
                    }
                    advance_next_deadline_unlocked(*job, fired_at);
                    rearm = true;
                } else if (job->overlap_policy == OverlapPolicy::Skip && job->in_flight > 0) {
                    ++job->skipped_count;
                    advance_next_deadline_unlocked(*job, fired_at);
                    rearm = true;
                } else {
                    update_job_stats(*job, error_us);
                    update_global_stats_unlocked(std::abs(error_us), error_us > 0.0);
                    ++job->in_flight;
                    ++total_in_flight_;
                    ++pool->queued;
                    ++pool->submitted;
                    dispatch = true;

                    if (job->overlap_policy == OverlapPolicy::Serial) {
                        rearm = false;
                    } else {
                        if (consume_remaining_run_unlocked(*job)) {
                            mark_job_schedule_complete_unlocked(*job);
                        }
                        if (job->active) {
                            advance_next_deadline_unlocked(*job, fired_at);
                            rearm = true;
                        }
                    }
                }

                stop_io = should_stop_io_unlocked();
            }

            if (rearm) {
                arm_job(job);
            } else if (stop_io && !dispatch) {
                std::lock_guard<std::mutex> lock(mutex_);
                stop_io_unlocked();
            }

            if (!dispatch) {
                return;
            }

            auto submitted_at = steady_clock_t::now();

            asio::post(pool->executor, [this, job, pool, fired_at, submitted_at]() {
                std::optional<std::string> error_message;

                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (pool->queued > 0) {
                        --pool->queued;
                    }
                    ++pool->running;
                    double queue_wait_us = std::chrono::duration<double, std::micro>(
                        steady_clock_t::now() - submitted_at
                    ).count();
                    update_job_queue_wait_stats(*job, queue_wait_us);
                }

                try {
                    invoke_job(*job);
                } catch (const py::error_already_set& e) {
                    error_message = e.what();
                } catch (const std::exception& e) {
                    error_message = e.what();
                } catch (...) {
                    error_message = std::string("unknown task exception");
                }

                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (pool->running > 0) {
                        --pool->running;
                    }
                    ++pool->completed;
                }

                asio::post(io_, [this, job, fired_at, error_message = std::move(error_message)]() mutable {
                    on_job_complete(job, fired_at, std::move(error_message));
                });
            });
        });
    }

    mutable std::mutex mutex_;
    std::vector<std::shared_ptr<Job>> jobs_;
    std::unordered_map<std::string, std::shared_ptr<PoolState>> pools_;

    asio::io_context io_;
    std::optional<asio::executor_work_guard<asio::io_context::executor_type>> work_guard_;
    std::thread worker_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};

    std::uint64_t total_runs_{0};
    std::uint64_t late_runs_{0};
    double max_late_us_{0.0};
    double mean_abs_error_us_{0.0};
    double m2_abs_error_{0.0};
    std::uint64_t total_in_flight_{0};

    std::uint64_t next_job_seq_{1};

    struct CurrentJobScope {
        explicit CurrentJobScope(const std::shared_ptr<Job>& job)
            : previous_(tls_current_job_) {
            tls_current_job_ = job;
        }

        ~CurrentJobScope() {
            tls_current_job_ = previous_;
        }

        std::weak_ptr<Job> previous_;
    };

    static thread_local std::weak_ptr<Job> tls_current_job_;
};

thread_local std::weak_ptr<Scheduler::Job> Scheduler::tls_current_job_{};

PYBIND11_MODULE(conductor, m) {
    py::class_<Scheduler>(m, "Scheduler")
        .def(py::init<std::uint64_t>(), py::arg("default_pool_workers") = 1)

        .def(
            "add_task",
            &Scheduler::add_task,
            py::arg("task"),
            py::arg("args") = py::none(),
            py::arg("period_us") = 1000,
            py::arg("start") = py::none(),
            py::arg("offset_us") = 0,
            py::arg("count") = py::none(),
            py::arg("task_id") = py::none(),
            py::arg("pool_id") = py::none(),
            py::arg("overlap_policy") = py::none()
        )

        .def("create_pool", &Scheduler::create_pool, py::arg("pool_id") = py::none(), py::arg("workers") = 1)
        .def("remove_pool", &Scheduler::remove_pool, py::arg("pool_id"))
        .def("list_pools", &Scheduler::list_pools)
        .def("read_pool_stats", &Scheduler::read_pool_stats, py::arg("pool_id") = py::none())
        .def("clear_tasks", &Scheduler::clear_tasks)
        .def("start_engine", &Scheduler::start_engine)
        .def("stop", &Scheduler::stop)
        .def("read_stats", &Scheduler::read_stats)
        .def("read_jobs", &Scheduler::read_jobs)
        .def(
            "suspend",
            &Scheduler::suspend,
            py::arg("task_id") = py::none(),
            py::arg("for_") = 0
        )
        .def(
            "resume",
            &Scheduler::resume,
            py::arg("task_id") = py::none(),
            py::arg("after") = 0
        );
}
