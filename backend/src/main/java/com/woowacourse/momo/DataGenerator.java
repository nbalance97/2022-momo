package com.woowacourse.momo;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.woowacourse.momo.category.domain.Category;

@Component
@RequiredArgsConstructor
public class DataGenerator {

    private static final Logger log = LoggerFactory.getLogger(DataGenerator.class);

    private static final int TOTAL_MEMBER_SIZE = 1_000_000;
    private static final int TOTAL_GROUP_SIZE = 1_000_000;
    private static final int BATCH_SIZE = 5_000;

    private static final int THREAD_COUNT = 5;

    ExecutorService executorService;
    private final JdbcTemplate jdbcTemplate;

    public void dbInit() throws InterruptedException {
        executorService = Executors.newFixedThreadPool(
                THREAD_COUNT
        );

        saveMembers();
        executorService.shutdown();
        while (!executorService.awaitTermination(100, TimeUnit.MILLISECONDS));

        executorService = Executors.newFixedThreadPool(
                THREAD_COUNT
        );
        saveGroups();
        executorService.shutdown();
        while (!executorService.awaitTermination(100, TimeUnit.MILLISECONDS));

        executorService = Executors.newFixedThreadPool(
                THREAD_COUNT
        );
        saveParticipants();
    }

    private void saveMembers() {
        final int threadBlockSize = TOTAL_MEMBER_SIZE / THREAD_COUNT;

        for (int thread = 0; thread < THREAD_COUNT; thread++) {
            final int threadIndex = thread;
            executorService.execute(() -> {
                log.info("Member 저장 시작-" + threadIndex);
                for (int i = 0; i < threadBlockSize / BATCH_SIZE; i++) {
                    String sql = "insert into momo_member(id, deleted, user_id, name, password) values (?, ?, ?, ?, ?)";
                    List<MemberDto> generatedMember = makeMember(
                            (threadBlockSize * threadIndex) + BATCH_SIZE * i + 1,
                            (threadBlockSize * threadIndex) + BATCH_SIZE * (i + 1)
                    );
                    jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            MemberDto member = generatedMember.get(i);
                            ps.setLong(1, member.getId());
                            ps.setBoolean(2, false);
                            ps.setString(3, member.getUserId());
                            ps.setString(4, member.getUserName());
                            ps.setString(5, member.getPassword());
                        }

                        @Override
                        public int getBatchSize() {
                            return generatedMember.size();
                        }
                    });
                }
                log.info("Member 저장 종료-" + threadIndex);
            });
        }
    }

    private List<MemberDto> makeMember(int src, int dest) {
        List<MemberDto> members = new ArrayList<>();
        for (long i = src; i <= dest; i++) {
            members.add(new MemberDto(i));
        }
        return members;
    }


    // 5000명의 호스트에 대해, 인당 100개의 모임 주최
    private void saveGroups() {
        for (int thread = 0; thread < THREAD_COUNT; thread++) {
            final int threadIndex = thread;
            final int threadBlockSize = TOTAL_GROUP_SIZE / THREAD_COUNT;
            executorService.execute(() -> {
                log.info("Group 저장 시작-" + threadIndex);
                for (int i = 0; i < threadBlockSize / BATCH_SIZE; i++) {
                    List<GroupDto> groupDtos = makeGroup(
                            (threadBlockSize * threadIndex) + BATCH_SIZE * i + 1,
                            (threadBlockSize * threadIndex) + BATCH_SIZE * (i + 1)
                    );
                    String sql =
                            "insert into momo_group(id, capacity, category, deadline, description, endDate, startDate, closedEarly, location, name, host_id) "
                                    + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            GroupDto groupDto = groupDtos.get(i);
                            ps.setLong(1, groupDto.getId());
                            ps.setInt(2, groupDto.getCapacity());
                            ps.setString(3, groupDto.getCategory());
                            ps.setDate(4, Date.valueOf(groupDto.getDeadline().toLocalDate()));
                            ps.setString(5, groupDto.getDescription());
                            ps.setDate(6, Date.valueOf(groupDto.getEndDate()));
                            ps.setDate(7, Date.valueOf(groupDto.getStartDate()));
                            ps.setBoolean(8, false);
                            ps.setString(9, "");
                            ps.setString(10, groupDto.getName());
                            ps.setLong(11, groupDto.getHostId());
                        }

                        @Override
                        public int getBatchSize() {
                            return groupDtos.size();
                        }
                    });
                }
                log.info("Group 저장 종료-" + threadIndex);
            });
        }
    }
    private List<GroupDto> makeGroup(int src, int dest) {
        List<GroupDto> groupDtos = new ArrayList<>();

        for (int i = src; i <= dest; i++) {
            long addTime = ThreadLocalRandom.current().nextLong(10);
            long minusTime = ThreadLocalRandom.current().nextLong(10);
            LocalDate startDate = LocalDate.now().plusDays(addTime).minusDays(minusTime);
            Category generatedCategory = Category.from(ThreadLocalRandom.current().nextLong(10) + 1);
            GroupDto groupDto = new GroupDto(
                    i,
                    5,
                    generatedCategory.name(),
                    LocalDateTime.of(startDate.minusDays(1), LocalTime.now()),
                    "설명",
                    startDate.plusDays(addTime),
                    startDate,
                    ThreadLocalRandom.current().nextBoolean(),
                    "",
                    "그룹" + i,
                    ThreadLocalRandom.current().nextInt(1000) + 1
            );
            groupDtos.add(groupDto);
        }

        return groupDtos;
    }

    @Getter
    private class MemberDto {
        private long id;
        private String userId;
        private String password;
        private String userName;

        public MemberDto(long id) {
            this.id = id;
            this.userId = "user" + id;
            this.password = "qwe123!" + id;
            this.userName = "name" + id;
        }
    }

    @Getter
    @AllArgsConstructor
    private class GroupDto {

        long id;
        int capacity;
        String category;
        LocalDateTime deadline;
        String description;
        LocalDate endDate;
        LocalDate startDate;
        boolean closedEarly;
        String location;
        String name;
        long hostId;
    }


    private static long participantId = 1;

    private void saveParticipants() {
        for (int thread = 0; thread < THREAD_COUNT; thread++) {
            final int threadNumber = thread;
            final int threadBlockSize = TOTAL_GROUP_SIZE / THREAD_COUNT;
            executorService.execute(() -> {
                log.info("Participant 저장 시작-" + threadNumber);
                for (int i = 0; i < threadBlockSize / BATCH_SIZE; i++) {
                    String sql = "insert into momo_participant(id, member_id, group_id) values (?, ?, ?)";
                    List<ParticipantDto> generatedParticipant = makeParticipant(
                            (threadBlockSize * threadNumber) + BATCH_SIZE * i + 1,
                            (threadBlockSize * threadNumber) + BATCH_SIZE * (i + 1)
                    );

                    jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            ParticipantDto participant = generatedParticipant.get(i);
                            ps.setLong(1, participant.getId());
                            ps.setLong(2, participant.getMember_id());
                            ps.setLong(3, participant.getGroup_id());
                        }

                        @Override
                        public int getBatchSize() {
                            return generatedParticipant.size();
                        }
                    });
                }
                log.info("Participant 저장 완료-" + threadNumber);
            });
        }
    }

    private List<ParticipantDto> makeParticipant(int src, int dest) {
        List<ParticipantDto> participants = new ArrayList<>();
        for (long groupId = src; groupId <= dest; groupId++) {
            for (long memberId = 1001; memberId < 1005; memberId++) {
                long id = 0;
                synchronized(this) {
                    id = participantId++;
                }
                participants.add(new ParticipantDto(id, memberId, groupId));
            }
        }
        return participants;
    }

    @Getter
    private class ParticipantDto {
        private long id;
        private long member_id;
        private long group_id;

        public ParticipantDto(long id, long member_id, long group_id) {
            this.id = id;
            this.member_id = member_id;
            this.group_id = group_id;
        }
    }
}